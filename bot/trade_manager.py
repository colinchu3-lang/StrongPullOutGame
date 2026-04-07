"""
trade_manager.py — V13 Fusion order lifecycle manager.

Key differences from generic trade manager:
  - Asymmetric TP/SL for long vs short
  - Long breakeven trailing stop (trigger at +40, lock at +30)
  - Stop guard: SL not active for N minutes after entry
  - Blocked entry windows (10:00-10:15 long, 10:30-10:45 short)
  - Direction-based cooldown (5 min long, 15 min short)
  - Circuit breakers: monthly DD limit, consecutive loss pause
  - Short can flip an open long (bypass cooldown)
"""
import asyncio
import csv
import logging
from datetime import datetime, time as dt_time, timedelta
from typing import Optional, Dict
from ib_async import IB, MarketOrder, LimitOrder, StopOrder, Trade

import config

log = logging.getLogger(__name__)


class TradeManager:

    def __init__(self, ib: IB, contract, portfolio):
        self.ib = ib
        self.contract = contract
        self.portfolio = portfolio

        self.active_trade: Optional[Dict] = None
        self.trade_log: list = []

        self.virtual_cash: float = config.INITIAL_VIRTUAL_CASH

        # Bracket order IDs for live mode
        self._parent_order_id: Optional[int] = None
        self._tp_order_id: Optional[int] = None
        self._sl_order_id: Optional[int] = None
        self._entry_placed_at: Optional[datetime] = None

        # Direction-based cooldown
        self._last_exit_time: Optional[datetime] = None
        self._last_exit_direction: Optional[str] = None  # 'LONG' or 'SHORT'

        # Circuit breakers
        self._monthly_pnl: float = 0.0
        self._current_month: Optional[int] = None  # YYYYMM
        self._month_halted: bool = False
        self._consec_losses: int = 0
        self._consec_pause_until: Optional[datetime] = None

        # Long breakeven trail state
        self._be_triggered: bool = False
        self._original_sl_price: float = 0.0
        self._real_sl_price: float = 0.0  # real SL to apply after guard period

        # Stop guard state
        self._stop_guard_active: bool = True  # starts True, becomes False after guard period

        if not config.SIMULATE_EXECUTION:
            self.ib.orderStatusEvent += self._on_order_status

        self._init_csv()

    # ── Time helpers ───────────────────────────────────────────

    def _now(self) -> datetime:
        return datetime.now(config.TZ)

    def _minutes_of_day(self, dt: datetime = None) -> int:
        if dt is None:
            dt = self._now()
        return dt.hour * 60 + dt.minute

    def is_in_kill_zone(self) -> bool:
        now = self._now()
        if now.weekday() > 4:
            return False
        mod = self._minutes_of_day(now)
        kz_start = config.KZ_START_HOUR * 60 + config.KZ_START_MINUTE
        kz_end = config.KZ_END_HOUR * 60 + config.KZ_END_MINUTE
        return kz_start <= mod <= kz_end

    def is_rth_now(self) -> bool:
        """Alias for kill zone check."""
        return self.is_in_kill_zone()

    def is_close_time(self) -> bool:
        now = self._now()
        if now.weekday() > 4:
            return False
        mod = self._minutes_of_day(now)
        eod = config.EOD_EXIT_HOUR * 60 + config.EOD_EXIT_MINUTE
        return mod >= eod

    def is_in_blocked_window(self, signal: str) -> bool:
        mod = self._minutes_of_day()
        if signal == 'LONG':
            return config.LONG_BLOCK_START_MINS <= mod < config.LONG_BLOCK_END_MINS
        elif signal == 'SHORT':
            return config.SHORT_BLOCK_START_MINS <= mod < config.SHORT_BLOCK_END_MINS
        return False

    # ── Cooldown (direction-based) ─────────────────────────────

    def is_in_cooldown(self) -> bool:
        if self._last_exit_time is None:
            return False
        cooldown = (config.LONG_COOLDOWN_SECONDS if self._last_exit_direction == 'LONG'
                    else config.SHORT_COOLDOWN_SECONDS)
        elapsed = (self._now() - self._last_exit_time).total_seconds()
        return elapsed < cooldown

    def cooldown_remaining(self) -> float:
        if self._last_exit_time is None:
            return 0.0
        cooldown = (config.LONG_COOLDOWN_SECONDS if self._last_exit_direction == 'LONG'
                    else config.SHORT_COOLDOWN_SECONDS)
        elapsed = (self._now() - self._last_exit_time).total_seconds()
        return max(0.0, cooldown - elapsed)

    # ── Circuit breakers ───────────────────────────────────────

    def _check_month_reset(self):
        now = self._now()
        month_id = now.year * 100 + now.month
        if self._current_month != month_id:
            self._current_month = month_id
            self._monthly_pnl = 0.0
            self._month_halted = False
            log.info(f"New month {month_id} — monthly PnL reset")

    def is_month_halted(self) -> bool:
        self._check_month_reset()
        return self._month_halted

    def is_consec_paused(self) -> bool:
        if self._consec_pause_until is None:
            return False
        return self._now() < self._consec_pause_until

    # ── Stop guard check ──────────────────────────────────────

    def is_stop_guard_expired(self) -> bool:
        """Returns True if SL is now active (guard period has passed)."""
        if self.active_trade is None:
            return True
        entry_time = self.active_trade.get('entry_time')
        if entry_time is None:
            return True
        guard_mins = (config.LONG_STOP_GUARD_MINS if self.active_trade['signal'] == 'LONG'
                      else config.SHORT_STOP_GUARD_MINS)
        elapsed = (self._now() - entry_time).total_seconds()
        return elapsed >= guard_mins * 60

    # ── Entry ──────────────────────────────────────────────────

    async def open_trade(self, signal: str, price: float, ts: datetime) -> bool:
        if self.active_trade is not None:
            return False

        if self.is_month_halted():
            log.info("Trade skipped — month halted (DD limit)")
            return False

        if self.is_consec_paused():
            remaining = (self._consec_pause_until - self._now()).total_seconds() / 60
            log.info(f"Trade skipped — consec loss pause ({remaining:.0f} min left)")
            return False

        if self.is_in_blocked_window(signal):
            log.info(f"Trade skipped — {signal} blocked window")
            return False

        # DOW filter: shorts blocked on Wednesday
        if signal == 'SHORT' and self._now().weekday() in config.SHORT_BLOCKED_DAYS:
            log.info(f"Trade skipped — SHORT blocked on {self._now().strftime('%A')}")
            return False

        action = 'BUY' if signal == 'LONG' else 'SELL'

        if signal == 'LONG':
            tp = price + config.LONG_TP_POINTS
            sl = price - config.LONG_SL_POINTS
        else:
            tp = price - config.SHORT_TP_POINTS
            sl = price + config.SHORT_SL_POINTS

        # Size
        if config.SIMULATE_EXECUTION:
            qty = self._calc_contracts(self.virtual_cash)
        else:
            qty = self.portfolio.max_contracts()

        if qty < 1:
            log.warning(f"Insufficient margin — skipping {signal}")
            return False

        self.active_trade = {
            'action': action,
            'signal': signal,
            'qty': qty,
            'entry': price,
            'tp': tp,
            'sl': sl,
            'entry_time': ts,
        }

        self._be_triggered = False
        self._original_sl_price = sl
        self._stop_guard_active = True

        if config.SIMULATE_EXECUTION:
            guard_mins = (config.LONG_STOP_GUARD_MINS if signal == 'LONG'
                          else config.SHORT_STOP_GUARD_MINS)
            log.info(f"[SIM] ENTRY {action} {qty}ct @ {price:.2f} | "
                     f"TP={tp:.2f} SL={sl:.2f} | Guard={guard_mins}min"
                     f"{' | BE trail: +40→+30' if signal == 'LONG' else ''}")
        else:
            await self._place_bracket(action, qty, price, tp, sl)

        return True

    async def flip_to_short(self, price: float, ts: datetime) -> bool:
        """Close an open long and immediately enter short. Bypasses cooldown."""
        if self.active_trade is None or self.active_trade['signal'] != 'LONG':
            return False

        # DOW check — don't flip to short on blocked days
        if self._now().weekday() in config.SHORT_BLOCKED_DAYS:
            log.info(f"FLIP skipped — SHORT blocked on {self._now().strftime('%A')}")
            return False

        log.info(f"FLIP: closing LONG, entering SHORT @ {price:.2f}")

        # Close the long
        if config.SIMULATE_EXECUTION:
            t = self.active_trade
            diff = price - t['entry']
            self._close_trade('FLIP', price, ts)
        else:
            await self._flatten_live("FLIP_TO_SHORT")

        # Enter short immediately
        tp = price - config.SHORT_TP_POINTS
        sl = price + config.SHORT_SL_POINTS

        qty = self.portfolio.max_contracts() if not config.SIMULATE_EXECUTION \
            else self._calc_contracts(self.virtual_cash)
        if qty < 1:
            return False

        self.active_trade = {
            'action': 'SELL',
            'signal': 'SHORT',
            'qty': qty,
            'entry': price,
            'tp': tp,
            'sl': sl,
            'entry_time': ts,
        }
        self._be_triggered = False
        self._stop_guard_active = True

        if config.SIMULATE_EXECUTION:
            log.info(f"[SIM] FLIP SHORT {qty}ct @ {price:.2f} | TP={tp:.2f} SL={sl:.2f}")
        else:
            await self._place_bracket('SELL', qty, price, tp, sl)

        return True

    # ── Exit check (sim mode) ──────────────────────────────────

    def check_exit(self, bar: dict) -> Optional[str]:
        if self.active_trade is None:
            return None
        if not config.SIMULATE_EXECUTION:
            return None

        t = self.active_trade
        result = None
        exit_price = 0.0

        # Stop guard: SL not active until guard period expires
        entry_time = t.get('entry_time')
        bar_time = bar.get('date', self._now())
        guard_mins = (config.LONG_STOP_GUARD_MINS if t['signal'] == 'LONG'
                      else config.SHORT_STOP_GUARD_MINS)
        if entry_time and isinstance(bar_time, datetime) and isinstance(entry_time, datetime):
            stop_live = (bar_time - entry_time).total_seconds() >= guard_mins * 60
        else:
            stop_live = True

        if t['action'] == 'BUY':
            # Long breakeven trail
            if not self._be_triggered and bar['high'] >= t['entry'] + config.LONG_BE_TRIGGER_PTS:
                self._be_triggered = True
                new_sl = t['entry'] + config.LONG_BE_LOCK_PTS
                if new_sl > t['sl']:
                    t['sl'] = new_sl
                    log.info(f"[SIM] BE trail activated: SL moved to {new_sl:.2f} "
                             f"(entry+{config.LONG_BE_LOCK_PTS})")

            if stop_live and bar['low'] <= t['sl']:
                result, exit_price = 'LOSS' if t['sl'] < t['entry'] else 'WIN', t['sl']
            elif bar['high'] >= t['tp']:
                result, exit_price = 'WIN', t['tp']
        else:
            if stop_live and bar['high'] >= t['sl']:
                result, exit_price = 'LOSS', t['sl']
            elif bar['low'] <= t['tp']:
                result, exit_price = 'WIN', t['tp']

        if result:
            self._close_trade(result, exit_price, bar.get('date', self._now()))

        return result

    # ── Live tick-level trailing stop check ─────────────────────

    async def check_trail_live(self, live_price: float):
        """Called from tick check loop to update breakeven trail in live mode."""
        if self.active_trade is None:
            return
        if config.SIMULATE_EXECUTION:
            return
        if self.active_trade['signal'] != 'LONG':
            return
        if self._be_triggered:
            return

        t = self.active_trade
        if live_price >= t['entry'] + config.LONG_BE_TRIGGER_PTS:
            self._be_triggered = True
            new_sl = t['entry'] + config.LONG_BE_LOCK_PTS

            log.info(f"[LIVE] BE trail triggered @ {live_price:.2f} — "
                     f"modifying SL to {new_sl:.2f}")

            # Modify the SL order in IB
            if self._sl_order_id is not None:
                for trade_obj in self.ib.openTrades():
                    if trade_obj.order.orderId == self._sl_order_id:
                        trade_obj.order.auxPrice = new_sl
                        self.ib.placeOrder(self.contract, trade_obj.order)
                        t['sl'] = new_sl
                        log.info(f"[LIVE] SL order modified to {new_sl:.2f}")
                        break

    # ── IB fill events ─────────────────────────────────────────

    def _on_order_status(self, trade: Trade):
        if self.active_trade is None:
            return
        oid = trade.order.orderId
        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice

        if oid == self._parent_order_id and status == 'Filled':
            if fill_price > 0:
                old = self.active_trade['entry']
                slip = fill_price - old
                self.active_trade['entry'] = fill_price

                # Recalculate TP/SL from actual fill price to eliminate slippage drift
                signal = self.active_trade['signal']
                if signal == 'LONG':
                    new_tp = fill_price + config.LONG_TP_POINTS
                    new_sl = fill_price - config.LONG_SL_POINTS
                else:
                    new_tp = fill_price - config.SHORT_TP_POINTS
                    new_sl = fill_price + config.SHORT_SL_POINTS

                self.active_trade['tp'] = new_tp
                self.active_trade['sl'] = new_sl
                self._real_sl_price = new_sl  # update for guard tighten

                # Modify TP order to match fill
                if self._tp_order_id is not None:
                    for t in self.ib.openTrades():
                        if t.order.orderId == self._tp_order_id:
                            t.order.lmtPrice = new_tp
                            self.ib.placeOrder(self.contract, t.order)
                            break

                log.info(f"[LIVE] ENTRY FILLED @ {fill_price:.2f} (slip {slip:+.2f}) | "
                         f"TP adjusted to {new_tp:.2f} | SL target {new_sl:.2f}")
            return

        if oid == self._tp_order_id and status == 'Filled':
            result = self._determine_result(fill_price)
            self._close_trade(result, fill_price, self._now())
            self._clear_bracket_ids()
            return

        if oid == self._sl_order_id and status == 'Filled':
            result = self._determine_result(fill_price)
            self._close_trade(result, fill_price, self._now())
            self._clear_bracket_ids()
            return

    # ── Internal close logic ───────────────────────────────────

    def _determine_result(self, exit_price: float) -> str:
        if self.active_trade is None:
            return 'UNKNOWN'
        t = self.active_trade
        diff = exit_price - t['entry']
        points = diff if t['action'] == 'BUY' else -diff
        return 'WIN' if points > 0 else 'LOSS'

    def _close_trade(self, result: str, exit_price: float, ts):
        t = self.active_trade
        qty = t['qty']
        diff = exit_price - t['entry']
        points = diff if t['action'] == 'BUY' else -diff
        pnl = points * qty * config.TICK_VALUE

        self.virtual_cash += pnl

        record = {
            'entry_time': t['entry_time'],
            'exit_time': ts,
            'signal': t['signal'],
            'qty': qty,
            'entry': t['entry'],
            'exit': exit_price,
            'points': round(points, 2),
            'pnl': round(pnl, 2),
            'result': result,
            'balance': round(self.virtual_cash, 2),
        }
        self.trade_log.append(record)
        self._write_csv(record)

        mode = 'SIM' if config.SIMULATE_EXECUTION else 'LIVE'
        log.info(f"[{mode}] EXIT {result} | {t['signal']} {qty}ct | "
                 f"Pts: {points:+.2f} | PnL: ${pnl:+,.2f} | Bal: ${self.virtual_cash:,.2f}")

        # Update circuit breakers
        self._check_month_reset()
        self._monthly_pnl += pnl
        if pnl <= 0:
            self._consec_losses += 1
            if self._consec_losses >= config.CONSEC_LOSS_PAUSE:
                self._consec_pause_until = self._now() + timedelta(hours=config.CONSEC_LOSS_HOURS)
                self._consec_losses = 0
                log.warning(f"CIRCUIT BREAKER: {config.CONSEC_LOSS_PAUSE} consec losses — "
                            f"paused until {self._consec_pause_until.strftime('%H:%M')}")
        else:
            self._consec_losses = 0

        if self._monthly_pnl <= -config.MONTHLY_DD_LIMIT:
            self._month_halted = True
            log.warning(f"CIRCUIT BREAKER: monthly DD ${self._monthly_pnl:,.2f} — halted for month")

        # Direction-based cooldown
        self._last_exit_time = self._now()
        self._last_exit_direction = t['signal']

        self.active_trade = None

    # ── Bracket order placement ────────────────────────────────

    async def _place_bracket(self, action, qty, entry_price, tp_price, sl_price):
        """
        Place bracket with market entry. During the stop guard period,
        the SL is set to a 200pt catastrophic level — far enough to never
        trigger on normal volatility, but still protects against flash crashes.
        After the guard expires, we tighten it to the real level.
        """
        signal = self.active_trade['signal'] if self.active_trade else '?'
        guard_mins = (config.LONG_STOP_GUARD_MINS if signal == 'LONG'
                      else config.SHORT_STOP_GUARD_MINS)

        # Catastrophic SL during guard: 200 pts away (covers tail risk only)
        CATASTROPHIC_DISTANCE = 200.0
        if signal == 'LONG':
            wide_sl = entry_price - CATASTROPHIC_DISTANCE
        else:
            wide_sl = entry_price + CATASTROPHIC_DISTANCE

        bracket = self.ib.bracketOrder(
            action, qty,
            limitPrice=entry_price,
            takeProfitPrice=tp_price,
            stopLossPrice=wide_sl,
        )

        parent = bracket[0]
        parent.orderType = 'MKT'
        parent.lmtPrice = 0
        parent.tif = 'DAY'
        bracket[1].tif = 'GTC'  # TP
        bracket[2].tif = 'GTC'  # SL

        trades = []
        for order in bracket:
            trade = self.ib.placeOrder(self.contract, order)
            trades.append(trade)

        self._parent_order_id = trades[0].order.orderId
        self._tp_order_id = trades[1].order.orderId
        self._sl_order_id = trades[2].order.orderId
        self._entry_placed_at = self._now()
        self._real_sl_price = sl_price  # the real SL to apply after guard

        log.info(f"[LIVE] MKT bracket: {action} {qty}ct | "
                 f"TP={tp_price:.2f} SL(catastrophic)={wide_sl:.2f} → "
                 f"SL(real)={sl_price:.2f} after {guard_mins}min guard | "
                 f"IDs: p={self._parent_order_id} tp={self._tp_order_id} sl={self._sl_order_id}")

        # Schedule SL tightening after guard period
        asyncio.create_task(self._tighten_sl_after_guard(guard_mins, sl_price))

    async def _tighten_sl_after_guard(self, guard_mins: int, real_sl: float):
        """Wait for the guard period, then modify the SL order to the real level."""
        await asyncio.sleep(guard_mins * 60)

        if self.active_trade is None:
            return  # trade already closed

        # If BE trail already triggered and moved SL tighter, don't widen it
        current_sl = self.active_trade.get('sl', real_sl)
        if self.active_trade['signal'] == 'LONG' and self._be_triggered:
            if current_sl > real_sl:
                log.info(f"[LIVE] Guard expired but BE trail already active (SL={current_sl:.2f})")
                return

        if self._sl_order_id is not None:
            for trade_obj in self.ib.openTrades():
                if trade_obj.order.orderId == self._sl_order_id:
                    trade_obj.order.auxPrice = real_sl
                    self.ib.placeOrder(self.contract, trade_obj.order)
                    self.active_trade['sl'] = real_sl
                    log.info(f"[LIVE] Guard expired — SL tightened to {real_sl:.2f}")
                    break

    def _clear_bracket_ids(self):
        self._parent_order_id = None
        self._tp_order_id = None
        self._sl_order_id = None
        self._entry_placed_at = None

    # ── EOD Flatten ────────────────────────────────────────────

    async def flatten_all(self, reason: str = "EOD", last_price: float = 0.0):
        if config.SIMULATE_EXECUTION:
            if self.active_trade is None:
                return
            exit_price = last_price if last_price > 0 else self.active_trade['entry']
            self._close_trade('FLAT', exit_price, self._now())
        else:
            await self._flatten_live(reason)

    async def _flatten_live(self, reason: str):
        log.info(f"[LIVE] FLATTEN ({reason})")
        open_trades = self.ib.openTrades()
        for t in open_trades:
            if getattr(t.contract, 'conId', None) == self.contract.conId:
                try:
                    self.ib.cancelOrder(t.order)
                except Exception:
                    pass

        local_sym = self.contract.localSymbol
        pos = self.portfolio.get_position(local_sym)
        if pos and pos.qty != 0:
            close_action = 'SELL' if pos.qty > 0 else 'BUY'
            close_qty = int(abs(pos.qty))
            order = MarketOrder(close_action, close_qty)
            order.tif = 'IOC'
            self.ib.placeOrder(self.contract, order)

        if self.active_trade:
            self.active_trade = None
            self._clear_bracket_ids()

    def _calc_contracts(self, cash: float) -> int:
        if cash <= 0:
            return 0
        if config.COMPOUND:
            raw = int(cash / config.CAPITAL_PER_CONTRACT) + 1
            return min(max(1, raw), config.MAX_CONTRACTS)
        margin = self.portfolio.margin_per_contract if self.portfolio else config.MARGIN_PER_CONTRACT_FALLBACK
        return min(int(cash // margin), config.MAX_CONTRACTS)

    # ── Broker sync ────────────────────────────────────────────

    async def sync_with_broker(self):
        if config.SIMULATE_EXECUTION or self.active_trade is None:
            return
        if self._entry_placed_at:
            elapsed = (self._now() - self._entry_placed_at).total_seconds()
            if elapsed < 60:
                return
        local_sym = self.contract.localSymbol
        if not self.portfolio.has_open_position(local_sym):
            t = self.active_trade
            exit_price = t['entry']
            for trade_obj in self.ib.trades():
                oid = trade_obj.order.orderId
                if oid in (self._tp_order_id, self._sl_order_id):
                    if trade_obj.orderStatus.status == 'Filled':
                        exit_price = trade_obj.orderStatus.avgFillPrice
                        break
            result = self._determine_result(exit_price)
            self._close_trade(result, exit_price, self._now())
            self._clear_bracket_ids()

    # ── CSV / Stats ────────────────────────────────────────────

    def _init_csv(self):
        try:
            with open(config.REPORT_FILE, 'x', newline='') as f:
                w = csv.DictWriter(f, fieldnames=[
                    'entry_time', 'exit_time', 'signal', 'qty',
                    'entry', 'exit', 'points', 'pnl', 'result', 'balance',
                ])
                w.writeheader()
        except FileExistsError:
            pass

    @staticmethod
    def _write_csv(record: dict):
        with open(config.REPORT_FILE, 'a', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(record.keys()))
            w.writerow(record)

    def stats_summary(self) -> str:
        if not self.trade_log:
            return "No trades yet."
        wins = sum(1 for t in self.trade_log if t['result'] == 'WIN')
        losses = sum(1 for t in self.trade_log if t['result'] in ('LOSS',))
        total_pnl = sum(t['pnl'] for t in self.trade_log)
        total = wins + losses
        wr = wins / total * 100 if total > 0 else 0
        return (f"Trades: {len(self.trade_log)} | W: {wins} L: {losses} | "
                f"Win%: {wr:.1f}% | PnL: ${total_pnl:+,.2f} | "
                f"MonthPnL: ${self._monthly_pnl:+,.2f}")