"""
ib_utils.py — Connection manager + contract factory.
All trading logic has moved to trade_manager.py.
"""
import logging
from ib_async import IB, Future, ContFuture, util
import config

log = logging.getLogger(__name__)


class IBConnection:
    """Thin wrapper around ib_async.IB for connect / disconnect / contract."""

    def __init__(self):
        self.ib = IB()
        self.contract = None

    # ── lifecycle ─────────────────────────────────────────────
    async def connect(self):
        await self.ib.connectAsync(
            config.GATEWAY_HOST,
            config.GATEWAY_PORT,
            clientId=config.CLIENT_ID,
            timeout=30,
        )
        # Give IB more time for requests (default is 2-4s, too short for paper)
        self.ib.RequestTimeout = 30

        self.contract = await self._resolve_front_month()
        log.info(f"Connected — qualified {self.contract.localSymbol} "
                 f"(conId={self.contract.conId}, "
                 f"expiry={self.contract.lastTradeDateOrContractMonth})")

    async def _resolve_front_month(self) -> Future:
        """
        Auto-resolve the front-month (most liquid) contract.
        
        Strategy:
        1. Use ContFuture (continuous future) to find the current front month
        2. If that fails, request contract details for the symbol and pick
           the nearest expiry with the most open interest
        3. Last resort: fall back to LOCAL_SYMBOL from config
        """
        # ── Attempt 1: ContFuture → resolves to front month ──
        try:
            cont = ContFuture(
                symbol=config.SYMBOL,
                exchange=config.EXCHANGE,
                currency=config.CURRENCY,
            )
            qualified = await self.ib.qualifyContractsAsync(cont)
            if qualified:
                # ContFuture gives us the conId of the front month
                # Now get the actual Future contract details
                details = await self.ib.reqContractDetailsAsync(qualified[0])
                if details:
                    real_contract = details[0].contract
                    # Re-qualify as a proper Future
                    fut = Future(conId=real_contract.conId,
                                 exchange=config.EXCHANGE)
                    q = await self.ib.qualifyContractsAsync(fut)
                    if q:
                        log.info(f"Auto-resolved front month via ContFuture: "
                                 f"{q[0].localSymbol}")
                        return q[0]
        except Exception as e:
            log.debug(f"ContFuture resolution failed: {e}")

        # ── Attempt 2: Search all expiries, pick nearest ──
        try:
            search = Future(
                symbol=config.SYMBOL,
                exchange=config.EXCHANGE,
                currency=config.CURRENCY,
            )
            details = await self.ib.reqContractDetailsAsync(search)
            if details:
                # Sort by expiry date, pick the nearest one
                details.sort(key=lambda d: d.contract.lastTradeDateOrContractMonth)
                nearest = details[0].contract
                fut = Future(conId=nearest.conId, exchange=config.EXCHANGE)
                q = await self.ib.qualifyContractsAsync(fut)
                if q:
                    log.info(f"Auto-resolved front month via nearest expiry: "
                             f"{q[0].localSymbol}")
                    return q[0]
        except Exception as e:
            log.debug(f"Nearest-expiry resolution failed: {e}")

        # ── Attempt 3: Fall back to config LOCAL_SYMBOL ──
        log.warning(f"Auto-resolution failed — falling back to {config.LOCAL_SYMBOL}")
        fallback = Future(
            localSymbol=config.LOCAL_SYMBOL,
            exchange=config.EXCHANGE,
            currency=config.CURRENCY,
        )
        qualified = await self.ib.qualifyContractsAsync(fallback)
        if not qualified:
            raise RuntimeError(
                f"Could not qualify any contract for {config.SYMBOL} / "
                f"{config.LOCAL_SYMBOL}")
        return qualified[0]

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            log.info("Disconnected from IB Gateway")

    def is_connected(self) -> bool:
        return self.ib.isConnected()

    async def reconnect(self) -> bool:
        """
        Attempt to reconnect to IB Gateway.
        Retries up to RECONNECT_MAX_TRIES with RECONNECT_DELAY between attempts.
        Returns True if reconnected successfully.
        """
        import asyncio

        for attempt in range(1, config.RECONNECT_MAX_TRIES + 1):
            log.info(f"Reconnect attempt {attempt}/{config.RECONNECT_MAX_TRIES}...")

            try:
                # Disconnect cleanly first if still partially connected
                try:
                    self.ib.disconnect()
                except Exception:
                    pass

                await asyncio.sleep(2)

                await self.ib.connectAsync(
                    config.GATEWAY_HOST,
                    config.GATEWAY_PORT,
                    clientId=config.CLIENT_ID,
                    timeout=30,
                )
                self.ib.RequestTimeout = 30

                # Re-qualify the contract
                if self.contract and self.contract.conId:
                    from ib_async import Future
                    fut = Future(conId=self.contract.conId,
                                 exchange=config.EXCHANGE)
                    q = await self.ib.qualifyContractsAsync(fut)
                    if q:
                        self.contract = q[0]

                log.info(f"Reconnected on attempt {attempt} — "
                         f"{self.contract.localSymbol} qualified")
                return True

            except Exception as e:
                log.warning(f"Reconnect attempt {attempt} failed: {e}")
                await asyncio.sleep(config.RECONNECT_DELAY)

        log.error(f"Failed to reconnect after {config.RECONNECT_MAX_TRIES} attempts")
        return False

    # ── helpers ───────────────────────────────────────────────
    async def get_historical_bars(self, duration='1 D', bar_size='1 min'):
        """Fetch historical bars as a DataFrame."""
        bars = await self.ib.reqHistoricalDataAsync(
            self.contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=False,
        )
        return util.df(bars)