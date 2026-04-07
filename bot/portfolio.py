"""
portfolio.py — Live portfolio tracker.

Subscribes to IB account updates and exposes real-time:
  - Net liquidation value
  - Available cash
  - Unrealized / realized P&L
  - Current positions
  - Per-contract initial margin (fetched from IB via whatIfOrder)
"""
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional
from ib_async import IB, AccountValue, PortfolioItem, MarketOrder

import config

log = logging.getLogger(__name__)


@dataclass
class PositionInfo:
    symbol: str
    qty: float
    avg_cost: float
    market_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


class Portfolio:
    """
    Real-time portfolio mirror.
    Call `start()` after connecting to IB to begin receiving updates.
    """

    def __init__(self, ib: IB, contract=None):
        self.ib = ib
        self.contract = contract  # used for margin probe

        # Account-level values (updated in real time)
        self.net_liquidation: float = 0.0
        self.available_cash: float = 0.0
        self.buying_power: float = 0.0
        self.unrealized_pnl: float = 0.0
        self.realized_pnl: float = 0.0
        self.maintenance_margin: float = 0.0
        self.init_margin_req: float = 0.0  # total account initial margin

        # Per-contract margin from IB (fetched once via whatIfOrder)
        self.margin_per_contract: float = config.MARGIN_PER_CONTRACT_FALLBACK

        # Position map: localSymbol -> PositionInfo
        self.positions: Dict[str, PositionInfo] = {}

        self._started = False
        self._account = ''

    # ── lifecycle ─────────────────────────────────────────────

    async def start(self):
        """Subscribe to account + portfolio updates."""
        if self._started:
            return

        self.ib.accountValueEvent += self._on_account_value
        self.ib.updatePortfolioEvent += self._on_portfolio_update

        # Determine the managed account
        accounts = self.ib.managedAccounts()
        acct = accounts[0] if accounts else ''
        self._account = acct
        log.info(f"Account detected: '{acct}'")

        # Subscribe to account updates with a timeout
        try:
            await asyncio.wait_for(
                self.ib.reqAccountUpdatesAsync(acct),
                timeout=10
            )
            log.info("reqAccountUpdates completed")
        except asyncio.TimeoutError:
            log.warning("reqAccountUpdatesAsync timed out — continuing with events")
        except Exception as e:
            log.warning(f"reqAccountUpdatesAsync failed: {e} — continuing")

        self._started = True

        # Give events a moment to stream in
        await asyncio.sleep(2)

        # If events already populated values, great. If not, try account summary.
        if self.net_liquidation == 0:
            log.info("No account values from events yet, trying reqAccountSummaryAsync...")
            try:
                summary = await asyncio.wait_for(
                    self.ib.reqAccountSummaryAsync(),
                    timeout=10
                )
                for item in summary:
                    self._on_account_value(item)
                log.info(f"Seeded from summary: NetLiq=${self.net_liquidation:,.2f}")
            except asyncio.TimeoutError:
                log.warning("reqAccountSummaryAsync timed out")
            except Exception as e:
                log.warning(f"reqAccountSummaryAsync failed: {e}")

        # Last resort: read whatever accountValues IB already cached
        if self.net_liquidation == 0:
            log.info("Trying ib.accountValues() cache...")
            for av in self.ib.accountValues():
                self._on_account_value(av)

        log.info(f"Portfolio ready: NetLiq=${self.net_liquidation:,.2f} "
                 f"Cash=${self.available_cash:,.2f}")

        # Fetch real per-contract margin from IB
        await self._fetch_contract_margin()

    def stop(self):
        if not self._started:
            return
        try:
            self.ib.wrapper.accountUpdateEvent.clear()
        except Exception:
            pass
        self.ib.accountValueEvent -= self._on_account_value
        self.ib.updatePortfolioEvent -= self._on_portfolio_update
        self._started = False
        log.info("Portfolio tracking stopped")

    # ── margin probe ──────────────────────────────────────────

    async def _fetch_contract_margin(self):
        """
        Determine per-contract margin. Strategy:
        
        1. Primary: Calculate from BuyingPower — IB always provides this,
           even when markets are closed. For futures:
           max_contracts = BuyingPower / margin_per_contract
           so margin_per_contract = BuyingPower / max_contracts
           But we don't know max_contracts yet... so we use:
           margin ≈ NetLiq * (NetLiq / BuyingPower) if BuyingPower > 0
           
           Actually simplest: IB buying power for futures = 
           available_funds / initial_margin_per_contract * price
           We can just try placing a whatIf order, but that fails outside 
           market hours. So we use a direct approach:
           
           For futures, IB provides FullInitMarginReq and FullMaintMarginReq
           in account values. We look for those first.
        
        2. Secondary: whatIfOrder (only works during market hours)
        
        3. Fallback: config value
        """
        # ── Path 1: Look for per-contract margin in account values ──
        # IB sends these tags for futures accounts
        margin_from_account = await self._scan_account_margin()
        if margin_from_account and margin_from_account > 0:
            self.margin_per_contract = margin_from_account
            log.info(f"IB margin per contract (from account values): "
                     f"${self.margin_per_contract:,.2f}")
            return

        # ── Path 2: Try whatIfOrder (works during market hours) ──
        if self.contract is not None:
            whatif_margin = await self._try_whatif_margin()
            if whatif_margin and whatif_margin > 0:
                self.margin_per_contract = whatif_margin
                log.info(f"IB margin per contract (from whatIf): "
                         f"${self.margin_per_contract:,.2f}")
                return

        # ── Path 3: Estimate from buying power ──
        if self.buying_power > 0 and self.net_liquidation > 0:
            # For a futures account, max contracts ≈ BuyingPower / margin
            # IB gives us buying power directly. With no positions open,
            # a rough estimate: margin = NetLiq / floor(BuyingPower / NetLiq)
            # but this is circular. Better approach:
            # IB futures buying power ≈ excess liquidity / margin_per_contract
            # Since we have $50k net liq and $200k buying power, that implies
            # the account can hold ~4 contracts, so margin ≈ $50k/4 ≈ $12.5k
            leverage = self.buying_power / self.net_liquidation
            if leverage > 0:
                estimated_max = int(leverage)
                if estimated_max > 0:
                    est_margin = self.net_liquidation / estimated_max
                    self.margin_per_contract = est_margin
                    log.info(f"IB margin per contract (estimated from buying power): "
                             f"${self.margin_per_contract:,.2f} "
                             f"(leverage ~{leverage:.1f}x, ~{estimated_max} contracts)")
                    return

        log.warning(f"Could not determine margin from IB — "
                    f"using fallback ${config.MARGIN_PER_CONTRACT_FALLBACK:,.0f}")

    async def _scan_account_margin(self) -> float | None:
        """
        Scan account values for futures margin tags.
        IB sends: FullInitMarginReq, FullMaintMarginReq, InitMarginReq, etc.
        If we have positions, we can derive per-contract margin.
        If no positions, look for FullInitMarginReq-S (securities segment).
        """
        margin_tags = {}
        for av in self.ib.accountValues():
            if av.tag in ('FullInitMarginReq', 'InitMarginReq', 
                          'FullMaintMarginReq', 'MaintMarginReq',
                          'FullInitMarginReq-S', 'InitMarginReq-S'):
                try:
                    margin_tags[av.tag] = float(av.value)
                except (ValueError, TypeError):
                    pass

        if margin_tags:
            log.info(f"Account margin tags: {margin_tags}")

        # If we have open positions, derive per-contract from total margin
        total_pos = sum(abs(p.qty) for p in self.positions.values())
        if total_pos > 0:
            init_margin = margin_tags.get('FullInitMarginReq',
                          margin_tags.get('InitMarginReq', 0))
            if init_margin > 0:
                return init_margin / total_pos

        return None

    async def _try_whatif_margin(self) -> float | None:
        """Attempt whatIfOrder — only works during market hours."""
        import re
        captured = {}

        def _on_error(reqId, errorCode, errorString, contract):
            match = re.search(r'MARGIN REQ\s*\[([0-9.,]+)\s*USD\]', errorString)
            if match:
                try:
                    captured['value'] = float(match.group(1).replace(',', ''))
                except ValueError:
                    pass

        self.ib.errorEvent += _on_error
        try:
            order = MarketOrder('BUY', 1)
            order.whatIf = True
            order.tif = 'GTC'
            trade = self.ib.placeOrder(self.contract, order)

            for _ in range(30):  # 3 seconds
                await asyncio.sleep(0.1)
                if 'value' in captured:
                    return captured['value']
                status = getattr(trade.orderStatus, 'status', '')
                if status in ('Cancelled', 'Inactive'):
                    break

            # Check margin on trade objects
            for obj in [trade.orderStatus, trade.order]:
                for attr in ['initMarginChange', 'initMargin',
                             'initMarginBefore', 'initMarginAfter']:
                    val = getattr(obj, attr, None)
                    if val is not None:
                        try:
                            v = abs(float(val))
                            if v > 0 and str(val) != '1.7976931348623157E308':
                                return v
                        except (ValueError, TypeError):
                            pass

            if 'value' in captured:
                return captured['value']

            return None
        except Exception as e:
            log.debug(f"whatIf probe failed: {e}")
            return None
        finally:
            self.ib.errorEvent -= _on_error

    async def refresh_margin(self):
        """Re-fetch margin (call periodically if needed, e.g. once per session)."""
        await self._fetch_contract_margin()

    # ── event handlers ────────────────────────────────────────

    def _on_account_value(self, av: AccountValue):
        """Fires on every account field change."""
        tag, val = av.tag, av.value
        try:
            v = float(val)
        except (ValueError, TypeError):
            return

        if tag == 'NetLiquidation':
            self.net_liquidation = v
        elif tag == 'AvailableFunds':
            self.available_cash = v
        elif tag == 'BuyingPower':
            self.buying_power = v
        elif tag == 'UnrealizedPnL':
            self.unrealized_pnl = v
        elif tag == 'RealizedPnL':
            self.realized_pnl = v
        elif tag == 'MaintMarginReq':
            self.maintenance_margin = v
        elif tag == 'InitMarginReq':
            self.init_margin_req = v

    def _on_portfolio_update(self, item: PortfolioItem):
        """Fires when any position changes."""
        sym = item.contract.localSymbol or item.contract.symbol
        if item.position == 0:
            self.positions.pop(sym, None)
            log.info(f"Position closed: {sym}")
        else:
            # IB's averageCost = avg_fill_price × multiplier (for futures)
            # Divide by multiplier to get the actual per-share/per-unit price
            multiplier = int(item.contract.multiplier or 1)
            avg_price = item.averageCost / multiplier if multiplier > 0 else item.averageCost

            self.positions[sym] = PositionInfo(
                symbol=sym,
                qty=item.position,
                avg_cost=avg_price,
                market_price=item.marketPrice,
                market_value=item.marketValue,
                unrealized_pnl=item.unrealizedPNL,
                realized_pnl=item.realizedPNL,
            )

    # ── queries ───────────────────────────────────────────────

    def get_position(self, local_symbol: str) -> Optional[PositionInfo]:
        return self.positions.get(local_symbol)

    def has_open_position(self, local_symbol: str) -> bool:
        pos = self.positions.get(local_symbol)
        return pos is not None and pos.qty != 0

    def max_contracts(self) -> int:
        """How many contracts we can open using IB's actual margin rate."""
        cash = self.available_cash if self.available_cash > 0 else self.net_liquidation
        if cash <= 0:
            return 0
        return min(int(cash // self.margin_per_contract), config.MAX_CONTRACTS)

    # ── display ───────────────────────────────────────────────

    def summary(self) -> str:
        lines = [
            f"Net Liq:      ${self.net_liquidation:>12,.2f}",
            f"Cash:         ${self.available_cash:>12,.2f}",
            f"Buying Pwr:   ${self.buying_power:>12,.2f}",
            f"Init Margin:  ${self.init_margin_req:>12,.2f}",
            f"Maint Margin: ${self.maintenance_margin:>12,.2f}",
            f"Margin/ct:    ${self.margin_per_contract:>12,.2f}",
            f"Unreal P&L:   ${self.unrealized_pnl:>12,.2f}",
            f"Real P&L:     ${self.realized_pnl:>12,.2f}",
            f"Max Contracts: {self.max_contracts()}",
            f"Positions:     {len(self.positions)}",
        ]
        for sym, p in self.positions.items():
            lines.append(f"  {sym}: {p.qty:+.0f} @ {p.avg_cost:.2f} "
                         f"(mkt {p.market_price:.2f}, PnL ${p.unrealized_pnl:,.2f})")
        return '\n'.join(lines)