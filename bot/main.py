"""
main.py — V13 Fusion live trading bot.

Replicates the backtest logic as closely as possible:
  - Asymmetric long/short signals and exits
  - Kill zone: 9:45–11:15 AM ET only
  - Short only Mon/Tue
  - Blocked windows (10:00–10:15 long, 10:30–10:45 short)
  - Long breakeven trail (+40 trigger → +30 lock)
  - Stop guard (25 min long, 15 min short)
  - Direction-based cooldown (5 min long, 15 min short)
  - Short-flips-long priority (bypass cooldown)
  - Circuit breakers (monthly DD, consecutive losses)
  - EOD flatten at 3:55 PM
"""
import asyncio
import logging
import sys
from datetime import datetime

import pandas as pd

import config
from ib_utils import IBConnection
from portfolio import Portfolio
from ticker_stream import TickerStream
from trade_manager import TradeManager
from strategy import FusionStrategy

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s — %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger('bot')


class TradingBot:
    def __init__(self):
        self.conn = IBConnection()
        self.strategy = FusionStrategy()
        self.portfolio: Portfolio = None
        self.trades: TradeManager = None
        self.streamer: TickerStream = None
        self._disconnected = False
        self._reconnecting = False
        self._flattened_today = False

        self.bars_df: pd.DataFrame = pd.DataFrame()

    async def start(self):
        await self.conn.connect()
        ib = self.conn.ib
        ib.disconnectedEvent += self._on_disconnect
        await asyncio.sleep(3)

        self.portfolio = Portfolio(ib, contract=self.conn.contract)
        await self.portfolio.start()
        await asyncio.sleep(3)

        now = datetime.now(config.TZ)
        log.info("=" * 60)
        log.info(f"  V13 FUSION BOT STARTED")
        log.info(f"  Time:      {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
        log.info(f"  Contract:  {self.conn.contract.localSymbol}")
        log.info(f"  Mode:      {'SIM' if config.SIMULATE_EXECUTION else 'LIVE'}")
        log.info(f"  ──────────────────────────────────────────────")
        log.info(f"  Net Liq:   ${self.portfolio.net_liquidation:>12,.2f}")
        log.info(f"  Max Ct:    {self.portfolio.max_contracts()}")
        log.info(f"  ──────────────────────────────────────────────")
        log.info(f"  LONG:  TP={config.LONG_TP_POINTS} SL={config.LONG_SL_POINTS} "
                 f"BE=+{config.LONG_BE_TRIGGER_PTS}→+{config.LONG_BE_LOCK_PTS} "
                 f"Guard={config.LONG_STOP_GUARD_MINS}min Cool=5min")
        log.info(f"  SHORT: TP={config.SHORT_TP_POINTS} SL={config.SHORT_SL_POINTS} "
                 f"Guard={config.SHORT_STOP_GUARD_MINS}min Cool=15min "
                 f"No Wed")
        log.info(f"  KZ:    {config.KZ_START_HOUR}:{config.KZ_START_MINUTE:02d}"
                 f"–{config.KZ_END_HOUR}:{config.KZ_END_MINUTE:02d} | "
                 f"EOD={config.EOD_EXIT_HOUR}:{config.EOD_EXIT_MINUTE:02d}")
        log.info(f"  Blocks: Long 10:00–10:15 | Short 10:30–10:45")
        log.info(f"  CB:    MonthDD=${config.MONTHLY_DD_LIMIT:,.0f} | "
                 f"{config.CONSEC_LOSS_PAUSE}L→{config.CONSEC_LOSS_HOURS}h pause")
        log.info("=" * 60)

        self.trades = TradeManager(ib, self.conn.contract, self.portfolio)

        if self.portfolio.net_liquidation > 0:
            self.trades.virtual_cash = self.portfolio.net_liquidation

        if not config.SIMULATE_EXECUTION:
            await asyncio.sleep(2)
            await self._recover_live_position()

        # Warmup
        await asyncio.sleep(2)
        log.info("Fetching warmup bars...")
        hist_df = await self.conn.get_historical_bars(
            duration=config.WARMUP_DURATION, bar_size='1 min')
        if hist_df is not None and len(hist_df) > 0:
            self.bars_df = hist_df.reset_index(drop=True)
            self.bars_df = self.strategy.calculate_indicators(self.bars_df)
            ind = self.strategy.get_indicator_summary(self.bars_df)
            log.info(f"Warmed up {len(self.bars_df)} bars | "
                     f"Close={ind.get('close', 0):.2f} RSI={ind.get('rsi', 0):.1f} "
                     f"Trend={ind.get('trend_60m', 0)} "
                     f"L_sig={ind.get('setup_long', 0):.0f} "
                     f"S_sig={ind.get('setup_short', 0):.0f}")

        self.streamer = TickerStream(self.conn)
        await self.streamer.start(
            on_bar=self._on_new_bar,
            on_tick_check=self._on_tick_check,
        )
        log.info("Bot is LIVE — streaming")

        # Heartbeat
        self._flattened_today = False
        now = datetime.now(config.TZ)
        secs = now.second + now.microsecond / 1_000_000
        target = 30 - secs if secs < 30 else 90 - secs
        await asyncio.sleep(target)

        try:
            while True:
                await asyncio.sleep(60)

                if self._disconnected and not self._reconnecting:
                    await self._handle_reconnect()
                    continue
                if not self.conn.is_connected():
                    self._disconnected = True
                    continue

                # EOD flatten
                if self.trades.is_close_time():
                    if not self._flattened_today:
                        log.info("EOD — flattening")
                        await self.trades.flatten_all("EOD", self.streamer.last_price)
                        self._flattened_today = True
                else:
                    self._flattened_today = False

                await self.trades.sync_with_broker()

                log.info(f"HB | {self.streamer.last_price:.2f} | "
                         f"KZ={'Y' if self.trades.is_in_kill_zone() else 'N'} | "
                         f"{self.trades.stats_summary()}")
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    # ── Bar callback ──────────────────────────────────────────

    async def _on_new_bar(self, bar: dict):
        if self._disconnected or self._reconnecting:
            return

        new_row = pd.DataFrame([bar])
        self.bars_df = pd.concat([self.bars_df, new_row], ignore_index=True)
        if len(self.bars_df) > config.ROLLING_WINDOW:
            self.bars_df = self.bars_df.tail(config.ROLLING_WINDOW).reset_index(drop=True)

        self.bars_df = self.strategy.calculate_indicators(self.bars_df)

        curr = self.bars_df.iloc[-1]
        ts = bar.get('date', datetime.now(config.TZ))
        dow = ts.weekday() if hasattr(ts, 'weekday') else datetime.now(config.TZ).weekday()

        ind = self.strategy.get_indicator_summary(self.bars_df)
        signal = self.strategy.check_signal(self.bars_df)

        log.info(f"Bar {curr['close']:.2f} | RSI={ind.get('rsi', 0):.1f} "
                 f"T60m={ind.get('trend_60m', 0)} "
                 f"L={ind.get('setup_long', 0):.0f} S={ind.get('setup_short', 0):.0f}"
                 f"{' → ' + signal if signal else ''}")

        # Check exits first (sim mode)
        self.trades.check_exit(bar)

        # SHORT flips LONG priority
        if (signal == 'SHORT'
                and self.trades.active_trade is not None
                and self.trades.active_trade['signal'] == 'LONG'
                and not self.trades.is_month_halted()
                and not self.trades.is_consec_paused()
                and dow not in config.SHORT_BLOCKED_DAYS):
            live_price = self.streamer.last_price if self.streamer else curr['close']
            entry_price = live_price if live_price > 0 else curr['close']
            await self.trades.flip_to_short(entry_price, ts)
            return

        # Normal entry — DOW filtering handled inside open_trade()
        if self.trades.active_trade is None and signal:
            if not self.trades.is_in_kill_zone():
                return

            # Cooldown check — but SHORT can bypass if last exit was LONG
            if self.trades.is_in_cooldown():
                if signal == 'SHORT' and self.trades._last_exit_direction == 'LONG':
                    log.info("SHORT bypassing LONG cooldown")
                else:
                    return

            live_price = self.streamer.last_price if self.streamer else curr['close']
            entry_price = live_price if live_price > 0 else curr['close']

            log.info(f"SIGNAL {signal} @ {entry_price:.2f} (DOW={dow})")
            await self.trades.open_trade(signal, entry_price, ts)

    # ── Tick check (every 5s) ──────────────────────────────────

    async def _on_tick_check(self, live_price: float, bar_snapshot: dict):
        if self._disconnected or self._reconnecting:
            return

        # Update breakeven trail on live ticks
        if self.trades.active_trade is not None:
            await self.trades.check_trail_live(live_price)
            return  # don't enter while in a position (flip handled in bar callback)

        if len(self.bars_df) < 2:
            return
        if not self.trades.is_in_kill_zone():
            return

        dow = datetime.now(config.TZ).weekday()

        signal = self.strategy.check_signal(self.bars_df)
        if not signal:
            return

        # Cooldown — SHORT can bypass if last exit was LONG
        if self.trades.is_in_cooldown():
            if signal == 'SHORT' and self.trades._last_exit_direction == 'LONG':
                pass  # bypass
            else:
                return

        ts = datetime.now(config.TZ)
        log.info(f"TICK ENTRY {signal} @ {live_price:.2f} (DOW={dow})")
        await self.trades.open_trade(signal, live_price, ts)

    # ── Shutdown / Reconnect / Recovery ───────────────────────

    async def shutdown(self):
        log.info("Shutting down...")
        if self.streamer:
            self.streamer.stop()
        if self.portfolio:
            self.portfolio.stop()
        self.conn.disconnect()
        log.info(f"Final: {self.trades.stats_summary()}")

    def _on_disconnect(self):
        if not self._reconnecting:
            log.error("IB connection LOST")
            self._disconnected = True

    async def _handle_reconnect(self):
        self._reconnecting = True
        log.info("RECONNECTING...")
        if self.streamer:
            self.streamer.stop()

        success = await self.conn.reconnect()
        if not success:
            log.error("Reconnect FAILED")
            self._reconnecting = False
            return

        ib = self.conn.ib
        ib.disconnectedEvent += self._on_disconnect

        try:
            self.portfolio = Portfolio(ib, contract=self.conn.contract)
            await self.portfolio.start()
            await asyncio.sleep(2)
        except Exception as e:
            log.warning(f"Portfolio resub failed: {e}")

        # Rebuild trade manager preserving state
        old_trade = self.trades.active_trade
        old_cash = self.trades.virtual_cash
        old_log = self.trades.trade_log
        old_exit_time = self.trades._last_exit_time
        old_exit_dir = self.trades._last_exit_direction
        old_monthly = self.trades._monthly_pnl
        old_month = self.trades._current_month
        old_halted = self.trades._month_halted
        old_consec = self.trades._consec_losses
        old_pause = self.trades._consec_pause_until

        self.trades = TradeManager(ib, self.conn.contract, self.portfolio)
        self.trades.virtual_cash = old_cash
        self.trades.trade_log = old_log
        self.trades._last_exit_time = old_exit_time
        self.trades._last_exit_direction = old_exit_dir
        self.trades._monthly_pnl = old_monthly
        self.trades._current_month = old_month
        self.trades._month_halted = old_halted
        self.trades._consec_losses = old_consec
        self.trades._consec_pause_until = old_pause

        if not config.SIMULATE_EXECUTION:
            await asyncio.sleep(2)
            await self._recover_live_position()
        elif old_trade:
            self.trades.active_trade = old_trade

        self.streamer = TickerStream(self.conn)
        await self.streamer.start(on_bar=self._on_new_bar, on_tick_check=self._on_tick_check)

        if self.portfolio.net_liquidation > 0:
            self.trades.virtual_cash = self.portfolio.net_liquidation

        self._disconnected = False
        self._reconnecting = False
        log.info(f"RECONNECTED | NetLiq=${self.portfolio.net_liquidation:,.2f}")

    async def _recover_live_position(self):
        ib = self.conn.ib
        local_sym = self.conn.contract.localSymbol
        con_id = self.conn.contract.conId

        if not self.portfolio.has_open_position(local_sym):
            log.info(f"No position for {local_sym}")
            return

        pos = self.portfolio.get_position(local_sym)
        action = 'BUY' if pos.qty > 0 else 'SELL'
        qty = int(abs(pos.qty))
        entry_price = pos.avg_cost
        signal = 'LONG' if pos.qty > 0 else 'SHORT'

        tp_price = sl_price = 0.0
        tp_oid = sl_oid = None

        for t in ib.openTrades():
            if getattr(t.contract, 'conId', None) != con_id:
                continue
            if t.orderStatus.status in ('Filled', 'Cancelled', 'Inactive'):
                continue
            if t.order.orderType == 'LMT':
                tp_price = t.order.lmtPrice
                tp_oid = t.order.orderId
            elif t.order.orderType == 'STP':
                sl_price = t.order.auxPrice
                sl_oid = t.order.orderId

        self.trades.active_trade = {
            'action': action, 'signal': signal, 'qty': qty,
            'entry': entry_price, 'tp': tp_price, 'sl': sl_price,
            'entry_time': datetime.now(config.TZ),
        }
        self.trades._tp_order_id = tp_oid
        self.trades._sl_order_id = sl_oid

        log.warning(f"Recovered: {signal} {qty}ct @ {entry_price:.2f} "
                    f"TP={tp_price:.2f} SL={sl_price:.2f}")


async def main():
    bot = TradingBot()
    await bot.start()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Manual stop.")