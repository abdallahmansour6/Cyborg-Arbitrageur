import os
import json
from datetime import datetime

LOG_FILE = "transaction.log"
STATE_FILE = "positions.json"
CLOSED_TRADES_FILE = "closed_trades.json"


def now_str():
    """Millisecond-precision timestamp matching the log format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def log(message, source="SYS"):
    """Prints a timestamped log and instantly writes to disk using OS buffering."""
    log_line = f"[{now_str()}] [{source.upper()}] {message}"
    print(log_line)

    try:
        # OS-level buffering makes this < 10 microseconds. No GIL threading penalty.
        with open(LOG_FILE, "a") as f:
            f.write(log_line + "\n")
    except Exception:
        pass


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):
    """Synchronous JSON dump. Tiny dicts take < 1 millisecond."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)


def append_closed_trade(record):
    """Append a closed-trade record to the archive. Creates the file if missing."""
    trades = []
    if os.path.exists(CLOSED_TRADES_FILE):
        try:
            with open(CLOSED_TRADES_FILE, "r") as f:
                trades = json.load(f)
        except Exception:
            trades = []
    trades.append(record)
    with open(CLOSED_TRADES_FILE, "w") as f:
        json.dump(trades, f, indent=4)
