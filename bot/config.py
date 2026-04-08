import os
from zoneinfo import ZoneInfo

# ─── Connection ───────────────────────────────────────────────
PAPER_TRADING  = True
GATEWAY_HOST   = 'ib-gateway'
GATEWAY_PORT   = 4004
CLIENT_ID      = 1
RECONNECT_DELAY    = 10
RECONNECT_MAX_TRIES = 30

# ─── Timezone ─────────────────────────────────────────────────
TZ = ZoneInfo('America/New_York')

# ─── Contract ─────────────────────────────────────────────────
SYMBOL         = 'MNQ'
LOCAL_SYMBOL   = 'MNQM6'       # Update each quarter (H/M/U/Z + year digit)
EXCHANGE       = 'CME'
CURRENCY       = 'USD'
TICK_VALUE     = 2.0            # MNQ = $2 per point

# ─── Execution Mode ──────────────────────────────────────────
SIMULATE_EXECUTION = False

# ─── Kill Zone (entry window) ─────────────────────────────────
KZ_START_HOUR    = 9
KZ_START_MINUTE  = 45
KZ_END_HOUR      = 11
KZ_END_MINUTE    = 15

# ─── EOD Flatten ──────────────────────────────────────────────
EOD_EXIT_HOUR    = 15
EOD_EXIT_MINUTE  = 55

# Aliases for trade_manager / main.py compatibility
RTH_ONLY           = True
TRADE_ETH          = False
RTH_START_HOUR     = KZ_START_HOUR
RTH_START_MINUTE   = KZ_START_MINUTE
RTH_END_HOUR       = KZ_END_HOUR
RTH_END_MINUTE     = KZ_END_MINUTE
MARKET_CLOSE_HOUR  = EOD_EXIT_HOUR
MARKET_CLOSE_MINUTE = EOD_EXIT_MINUTE

# ─── LONG Signal Params ──────────────────────────────────────
EMA8_PERIOD    = 8
EMA21_PERIOD   = 21
SMA200_PERIOD  = 200
RSI_PERIOD     = 14
RSI_OB         = 65             # Don't buy if RSI >= 65
PULLBACK_PCT   = 0.0015         # Low must touch within 0.15% of EMA21

# ─── SHORT Signal Params ─────────────────────────────────────
EMA_SLOPE_PERIOD   = 50
EMA_SLOPE_LOOKBACK = 20
SWING_LOOKBACK     = 10
ATR_PERIOD         = 14
ATR_AVG_PERIOD     = 20
CONFIRM_BARS       = 2
SHORT_BLOCKED_DAYS = [2]        # Block shorts on Wednesday (0=Mon..4=Fri)

# ─── TP / SL (asymmetric) ────────────────────────────────────
LONG_TP_POINTS     = 90
LONG_SL_POINTS     = 40
SHORT_TP_POINTS    = 90
SHORT_SL_POINTS    = 40

# ─── Long Breakeven Trail ────────────────────────────────────
LONG_BE_TRIGGER_PTS = 30.0      # once price reaches entry + 30, move SL
LONG_BE_LOCK_PTS    = 20.0      # SL moves to entry + 20

# ─── Short Breakeven Trail ───────────────────────────────────
SHORT_BE_TRIGGER_PTS = 30.0     # once price drops 30 from entry, move SL
SHORT_BE_LOCK_PTS    = 20.0     # SL moves to entry - 20

# ─── Stop Guard (SL inactive for N min after entry) ──────────
LONG_STOP_GUARD_MINS  = 25
SHORT_STOP_GUARD_MINS = 25

# ─── Blocked Entry Windows ───────────────────────────────────
LONG_BLOCK_START_MINS  = 600    # 10:00
LONG_BLOCK_END_MINS    = 615    # 10:15
SHORT_BLOCK_START_MINS = 630    # 10:30
SHORT_BLOCK_END_MINS   = 645    # 10:45

# ─── Cooldown (direction-based) ──────────────────────────────
LONG_COOLDOWN_SECONDS  = 300    # 5 min after a long exit
SHORT_COOLDOWN_SECONDS = 900    # 15 min after a short exit

# ─── Circuit Breakers ────────────────────────────────────────
MONTHLY_DD_LIMIT   = 3000.0
CONSEC_LOSS_PAUSE  = 3
CONSEC_LOSS_HOURS  = 4

# ─── Entry Order Settings ────────────────────────────────────
USE_LIMIT_ENTRY      = False    # Market order for fastest fill

# ─── Risk / Sizing ───────────────────────────────────────────
MARGIN_PER_CONTRACT_FALLBACK = 10_000.0
MAX_CONTRACTS                = 5
INITIAL_VIRTUAL_CASH         = 50_000.0
COMPOUND                     = True
CAPITAL_PER_CONTRACT         = 25_000.0

# ─── Tick Streaming ──────────────────────────────────────────
BAR_INTERVAL_SECONDS = 60
TICK_CHECK_SECONDS   = 5
WARMUP_DURATION      = '5 D'
ROLLING_WINDOW       = 3000

# ─── Logging / Reports ───────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
LOG_FILE       = os.path.join(BASE_DIR, 'ibkr_bot.log')
REPORT_FILE    = os.path.join(BASE_DIR, 'live_trade_report.csv')
SIGNAL_LOG     = os.path.join(BASE_DIR, 'signal_log.csv')
DAILY_SUMMARY  = os.path.join(BASE_DIR, 'daily_summary.log')