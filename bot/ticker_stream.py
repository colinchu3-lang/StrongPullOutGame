"""
ticker_stream.py — Subscribes to real-time market data ticks and
aggregates them into 1-minute OHLCV bars.

Two callback modes:
  - on_bar: fires every BAR_INTERVAL_SECONDS (1 min) with completed OHLCV bar
  - on_tick_check: fires every TICK_CHECK_SECONDS (15s) with current live price
    for fast entry/exit decisions between bars
"""
import asyncio
import logging
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd
from ib_async import Ticker

import config

log = logging.getLogger(__name__)


@dataclass
class BarBuilder:
    """Accumulates ticks within a single bar interval."""
    open: float = 0.0
    high: float = -float('inf')
    low: float = float('inf')
    close: float = 0.0
    volume: int = 0
    tick_count: int = 0
    bar_start: Optional[datetime] = None

    def reset(self, ts: datetime):
        self.open = 0.0
        self.high = -float('inf')
        self.low = float('inf')
        self.close = 0.0
        self.volume = 0
        self.tick_count = 0
        self.bar_start = ts

    def update(self, price: float, size: int = 0):
        if self.tick_count == 0:
            self.open = price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.tick_count += 1

    def is_empty(self) -> bool:
        return self.tick_count == 0

    def to_dict(self) -> dict:
        return {
            'date': self.bar_start,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
        }

    def snapshot(self) -> dict:
        """Current partial bar state (for mid-bar checks)."""
        return {
            'date': self.bar_start,
            'open': self.open,
            'high': self.high if self.high != -float('inf') else 0,
            'low': self.low if self.low != float('inf') else 0,
            'close': self.close,
            'volume': self.volume,
            'tick_count': self.tick_count,
        }


class TickerStream:
    """
    Streams live ticks for the configured contract.
    
    Two loops:
    - Bar timer: every 60s, emits completed 1-min bar → on_bar callback
    - Tick check: every 15s, fires on_tick_check with live price for fast decisions
    """

    def __init__(self, ib_conn):
        self.ib = ib_conn.ib
        self.contract = ib_conn.contract
        self.ticker: Optional[Ticker] = None
        self._bar = BarBuilder()
        self._running = False
        self._bar_task: Optional[asyncio.Task] = None
        self._tick_check_task: Optional[asyncio.Task] = None
        self._on_bar: Optional[Callable] = None
        self._on_tick_check: Optional[Callable] = None
        self._last_price: float = 0.0

    # ── public API ────────────────────────────────────────────

    async def start(self, on_bar: Callable, on_tick_check: Callable = None):
        """
        Begin streaming.
        
        on_bar(bar_dict): called every BAR_INTERVAL_SECONDS with completed bar.
        on_tick_check(price, bar_snapshot): called every TICK_CHECK_SECONDS
            with the current live price and partial bar data.
        """
        self._on_bar = on_bar
        self._on_tick_check = on_tick_check
        self._running = True

        # Subscribe to real-time ticks
        self.ticker = self.ib.reqMktData(
            self.contract,
            genericTickList='',
            snapshot=False,
            regulatorySnapshot=False,
        )
        self.ticker.updateEvent += self._on_tick
        log.info(f"Subscribed to live ticks for {self.contract.localSymbol}")

        # Start the bar-emission timer loop
        self._bar.reset(datetime.now(config.TZ))
        self._bar_task = asyncio.create_task(self._bar_timer())

        # Start the fast tick-check loop
        if self._on_tick_check:
            self._tick_check_task = asyncio.create_task(self._tick_check_loop())
            log.info(f"Fast tick check every {config.TICK_CHECK_SECONDS}s enabled")

    def stop(self):
        self._running = False
        if self.ticker:
            self.ib.cancelMktData(self.contract)
            log.info("Unsubscribed from live ticks")
        if self._bar_task and not self._bar_task.done():
            self._bar_task.cancel()
        if self._tick_check_task and not self._tick_check_task.done():
            self._tick_check_task.cancel()

    # ── tick handler ──────────────────────────────────────────

    def _on_tick(self, ticker: Ticker):
        """Called by ib_async on every tick update."""
        price = ticker.last
        if price is None or price != price:  # nan check
            price = ticker.close
        if price is None or price != price:
            return

        size = int(ticker.lastSize or 0)
        self._last_price = price
        self._bar.update(price, size)

    # ── bar emission loop (every 60s, aligned to clock minutes) ─

    async def _bar_timer(self):
        """
        Emits completed bars aligned to exact clock minutes.
        
        Bar for 12:00 = ticks from 12:00:00.000 to 12:00:59.999
        Emitted at 12:01:00.000 → bar timestamp = 12:00
        
        This matches ToS/TradingView 1-min bars exactly.
        """
        # Wait until the next exact minute boundary
        now = datetime.now(config.TZ)
        seconds_into_minute = now.second + now.microsecond / 1_000_000
        wait = 60 - seconds_into_minute
        if wait < 0.5:
            wait += 60  # too close to boundary, wait for next one

        log.debug(f"Bar timer: waiting {wait:.1f}s to align to next minute")
        await asyncio.sleep(wait)

        # Now we're at a clean minute boundary
        # Reset the bar with the current minute's timestamp
        bar_start = datetime.now(config.TZ).replace(second=0, microsecond=0)
        self._bar.reset(bar_start)

        while self._running:
            # Sleep until the next minute boundary
            now = datetime.now(config.TZ)
            seconds_into_minute = now.second + now.microsecond / 1_000_000
            wait = 60 - seconds_into_minute
            if wait < 0.1:
                wait += 60
            await asyncio.sleep(wait)

            # Emit the completed bar
            if not self._bar.is_empty() and self._on_bar:
                bar_data = self._bar.to_dict()
                try:
                    result = self._on_bar(bar_data)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    log.exception("Error in on_bar callback")

            # Reset for next bar — timestamp is the START of the new minute
            bar_start = datetime.now(config.TZ).replace(second=0, microsecond=0)
            self._bar.reset(bar_start)

    # ── fast tick check loop (every 15s) ──────────────────────

    async def _tick_check_loop(self):
        """Fires on_tick_check every TICK_CHECK_SECONDS with live price."""
        interval = config.TICK_CHECK_SECONDS

        # Small initial delay to let ticks start flowing
        await asyncio.sleep(2)

        while self._running:
            loop_start = time.time()

            if self._last_price > 0 and self._on_tick_check:
                try:
                    snapshot = self._bar.snapshot() if not self._bar.is_empty() else None
                    result = self._on_tick_check(self._last_price, snapshot)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    log.exception("Error in on_tick_check callback")

            elapsed = time.time() - loop_start
            await asyncio.sleep(max(0, interval - elapsed))

    # ── snapshot ──────────────────────────────────────────────

    @property
    def last_price(self) -> float:
        return self._last_price