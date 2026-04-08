"""
strategy.py — V13 Fusion Strategy.

Two completely independent signal systems:

LONG (EMA21 pullback with 2-bar confirmation):
  - 60m trend is bullish (EMA8 > EMA21 on 60-min)
  - Close > SMA200 (macro filter)
  - EMA8 > EMA21 on 1-min (bullish stack)
  - Low touched EMA21 zone (within 0.15%) and close recovered above EMA21
  - RSI < 65 (not overbought)
  - 2-bar confirmation: bar after setup must close above EMA8
  - Signal fires on the bar AFTER confirmation (enter at open)

SHORT (swing low breakdown with volatility filter):
  - Close < EMA200 (bearish macro)
  - EMA50 declining (current < 20 bars ago)
  - Close < swing low (10-bar lookback)
  - ATR > ATR average (volatility expansion)
  - 2-bar confirmation: both bars must meet conditions
  - Signal fires on the bar AFTER confirmation (enter at open)
  - Blocked on Wednesday (DOW filter applied in trade_manager)
"""
import pandas as pd
import numpy as np
import config


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Exact Wilder's RSI — matches TOS and TradingView.
    Seed: simple average of first `period` gains/losses.
    Smoothing: avg = (prev * (period-1) + current) / period
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))

    avg_gain = np.full(len(series), np.nan)
    avg_loss = np.full(len(series), np.nan)

    # Seed: simple mean of first `period` changes (bars 1..period)
    seed_slice = slice(1, period + 1)
    avg_gain[period] = gain.iloc[seed_slice].mean()
    avg_loss[period] = loss.iloc[seed_slice].mean()

    # Wilder smoothing from bar period+1 onward
    gain_vals = gain.values
    loss_vals = loss.values
    for i in range(period + 1, len(series)):
        avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gain_vals[i]) / period
        avg_loss[i] = (avg_loss[i - 1] * (period - 1) + loss_vals[i]) / period

    avg_gain = pd.Series(avg_gain, index=series.index)
    avg_loss = pd.Series(avg_loss, index=series.index)

    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


class FusionStrategy:
    """
    V13 Fusion: asymmetric long (EMA pullback) and short (swing breakdown).
    Computes indicators on rolling 1-min bar DataFrame and checks signals.
    """

    def __init__(self):
        pass

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all indicators on the 1-min bar DataFrame.
        Expects columns: date, open, high, low, close, volume
        """
        df = df.copy()

        # Ensure datetime index for resampling
        if 'date' in df.columns:
            df['_ts'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
            df_idx = df.set_index('_ts')
        else:
            df_idx = df

        # ── 60-min trend (shifted by 1 period) ───────────────
        df60 = df_idx['close'].resample('60min').last().dropna().to_frame()
        df60['ema8'] = df60['close'].ewm(span=8, adjust=False).mean()
        df60['ema21'] = df60['close'].ewm(span=21, adjust=False).mean()
        df60['trend_60m'] = np.where(df60['ema8'] > df60['ema21'], 1, -1)
        df60['trend_60m'] = df60['trend_60m'].shift(1)  # no lookahead

        trend_reindexed = df60['trend_60m'].reindex(df_idx.index, method='ffill')
        if 'date' in df.columns:
            df['trend_60m'] = trend_reindexed.values
        else:
            df['trend_60m'] = trend_reindexed
        df['trend_60m'] = df['trend_60m'].fillna(0).astype(int)

        # ── LONG indicators ───────────────────────────────────
        df['ema8'] = df['close'].ewm(span=config.EMA8_PERIOD, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=config.EMA21_PERIOD, adjust=False).mean()
        df['sma200'] = df['close'].rolling(config.SMA200_PERIOD).mean()
        df['rsi'] = _rsi(df['close'], config.RSI_PERIOD)

        # Pullback to EMA21
        ema21_upper = df['ema21'] * (1 + config.PULLBACK_PCT)
        touched_ema21 = (df['low'] <= ema21_upper) & (df['close'] >= df['ema21'])

        long_raw = (
            (df['trend_60m'] == 1) &
            (df['close'] > df['sma200']) &
            (df['ema8'] > df['ema21']) &
            touched_ema21 &
            (df['rsi'] < config.RSI_OB)
        )

        # 2-bar confirmation: bar after long_raw must close above EMA8
        confirms_ema8 = df['close'] > df['ema8']
        df['setup_long'] = (
            long_raw.shift(1).fillna(False) &
            confirms_ema8
        ).astype(float).shift(1).fillna(0)  # enter on NEXT bar's open

        # ── SHORT indicators ──────────────────────────────────
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['ema50'] = df['close'].ewm(span=config.EMA_SLOPE_PERIOD, adjust=False).mean()
        df['ema50_prev'] = df['ema50'].shift(config.EMA_SLOPE_LOOKBACK)

        tr = np.maximum(
            df['high'] - df['low'],
            np.maximum(
                abs(df['high'] - df['close'].shift(1)),
                abs(df['low'] - df['close'].shift(1))
            )
        )
        df['atr'] = tr.rolling(config.ATR_PERIOD).mean()
        df['atr_avg'] = df['atr'].rolling(config.ATR_AVG_PERIOD).mean()
        df['atr_ok'] = (df['atr'] > df['atr_avg']).astype(float)

        df['swing_low'] = df['low'].shift(1).rolling(config.SWING_LOOKBACK).min()

        bear = (df['close'] < df['ema200']) & (df['ema50'] < df['ema50_prev'])
        short_raw = bear & (df['close'] < df['swing_low']) & (df['atr_ok'] > 0.5)

        df['setup_short'] = (
            short_raw.astype(int)
            .rolling(config.CONFIRM_BARS).min()
            .fillna(0)
            .shift(1)
            .fillna(0)
            .astype(float)
        )

        # Clean up temp column
        if '_ts' in df.columns:
            df.drop(columns=['_ts'], inplace=True)

        return df

    def check_signal(self, df: pd.DataFrame) -> str | None:
        """
        Check the most recent bar for an entry signal.
        Returns 'LONG', 'SHORT', or None.
        DOW filtering is handled by trade_manager, not here.
        """
        if len(df) < 2:
            return None

        curr = df.iloc[-1]

        # Long signal
        if curr.get('setup_long', 0) > 0.5:
            return 'LONG'

        # Short signal
        if curr.get('setup_short', 0) > 0.5:
            return 'SHORT'

        return None

    def get_indicator_summary(self, df: pd.DataFrame) -> dict:
        """Return current indicator values for logging."""
        if len(df) < 1:
            return {}
        curr = df.iloc[-1]
        return {
            'close': curr.get('close', 0),
            'ema8': curr.get('ema8', float('nan')),
            'ema21': curr.get('ema21', float('nan')),
            'sma200': curr.get('sma200', float('nan')),
            'rsi': curr.get('rsi', float('nan')),
            'trend_60m': curr.get('trend_60m', 0),
            'setup_long': curr.get('setup_long', 0),
            'setup_short': curr.get('setup_short', 0),
        }