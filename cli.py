import argparse
import json
import sys
import urllib.request
import urllib.error
from datetime import datetime

# CHECKPOINT log: CLI Process Boot
boot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
print(f"[{boot_time}] [CLI] Process booted, parsing args...")

ENGINE_URL = "http://127.0.0.1:8080"


def parse_leg_spec(s: str):
    """Parse '<exchange>:<symbol>' into [exchange_lower, symbol].

    Splits on the FIRST colon — symbols themselves contain a colon
    (`CHEEMS/USDT:USDT`), so the split-on-first cleanly separates the
    exchange slug from the rest. Argparse delivers `s` as a single token
    because shell whitespace doesn't appear inside the spec.
    """
    if ":" not in s:
        raise argparse.ArgumentTypeError(
            f"Leg spec '{s}' must be 'exchange:symbol' (e.g. 'binance:1000CHEEMS/USDT:USDT')"
        )
    ex, sym = s.split(":", 1)
    if not ex or not sym:
        raise argparse.ArgumentTypeError(f"Leg spec '{s}' has empty exchange or symbol.")
    return [ex.lower(), sym]


def _render_success(res_data):
    """One-line ack from any Engine success payload.
    warmup/abort return {message}; entry/exit return {filled, target, halt_reason, [remaining], vwap_*_base}.
    """
    if "message" in res_data:
        return res_data["message"]
    parts = []
    if "filled" in res_data:
        parts.append(f"filled_base={res_data['filled']:.8f}/{res_data.get('target', '?')}")
    if "remaining" in res_data:
        parts.append(f"remaining_base={res_data['remaining']:.8f}")
    if "halt_reason" in res_data:
        parts.append(f"halt_reason={res_data['halt_reason']}")
    if "realized_basis_bps" in res_data:
        parts.append(f"realized_basis_bps={res_data['realized_basis_bps']:.2f}")
    return " | ".join(parts) if parts else json.dumps(res_data)


def send_command(endpoint, payload):
    url = f"{ENGINE_URL}/{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )

    try:
        # CHECKPOINT log: Payload Dispatch
        dispatch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[{dispatch_time}] [CLI] Dispatching IPC to Engine...")

        with urllib.request.urlopen(req) as response:
            res_data = json.loads(response.read().decode("utf-8"))
            print(f"✅ SUCCESS: {_render_success(res_data)}")

    except urllib.error.HTTPError as e:
        res_body = e.read().decode("utf-8")
        res_data = json.loads(res_body) if res_body else {}
        print(f"❌ ERROR: {res_data.get('error', e.reason)}")
        sys.exit(1)
    except urllib.error.URLError:
        print("🚨 CRITICAL: Cannot connect to the Engine. Is engine.py running?")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cyborg Arb CLI")
    subparsers = parser.add_subparsers(dest="action", required=True)

    # WARMUP COMMAND (Network Heavy - sets leverage, loads markets, keeps RAM hot)
    # Each leg passed as `exchange:symbol`. Symmetric pairs pass two legs with the same symbol;
    # asymmetric (multiplier-prefix) pairs pass two legs with different symbols.
    parser_warmup = subparsers.add_parser("warmup")
    parser_warmup.add_argument(
        "--legs",
        nargs="+",
        required=True,
        type=parse_leg_spec,
        help="Leg specs as 'exchange:symbol' (e.g. 'coinex:CHEEMS/USDT:USDT' "
             "'binance:1000CHEEMS/USDT:USDT'). Two legs for a hedged pair.",
    )
    parser_warmup.add_argument("--leverage", type=int, required=True)

    # ENTRY COMMAND (Drives Synchronized Smart Slicing on the Engine)
    parser_entry = subparsers.add_parser("entry")
    parser_entry.add_argument(
        "--long",
        required=True,
        type=parse_leg_spec,
        help="Long-leg spec: 'exchange:symbol' (e.g. 'coinex:CHEEMS/USDT:USDT')",
    )
    parser_entry.add_argument(
        "--short",
        required=True,
        type=parse_leg_spec,
        help="Short-leg spec: 'exchange:symbol' (e.g. 'binance:1000CHEEMS/USDT:USDT')",
    )
    parser_entry.add_argument(
        "--base-amount",
        dest="base_amount",
        type=float,
        required=True,
        help="Target true-1x base token amount of the underlying coin. "
             "Engine derives per-leg native qty from each leg's multiplier and contract_size.",
    )
    parser_entry.add_argument(
        "--min-entry-basis-bps",
        dest="min_entry_basis_bps",
        type=float,
        required=True,
        help="Net basis floor (bps) in per-1x-base price units. "
             "Slices below this are not dispatched.",
    )
    parser_entry.add_argument(
        "--max-duration-s",
        dest="max_duration_s",
        type=float,
        required=True,
        help="Hard wall-clock deadline. Partial fills are kept as a hedged position.",
    )

    # EXIT COMMAND (Drives reduceOnly Synchronized Smart Slicing on the Engine)
    # Position is identified by its base_coin (the multiplier-stripped underlying);
    # leg specs are looked up from saved state — no need to re-type them.
    parser_exit = subparsers.add_parser("exit")
    parser_exit.add_argument(
        "--pair",
        required=True,
        help="Position key: the base coin of the active position (e.g. 'CHEEMS', 'XRP').",
    )
    parser_exit.add_argument(
        "--base-amount",
        dest="base_amount",
        type=float,
        required=True,
        help="Base token amount to unwind (1x of underlying coin).",
    )
    parser_exit.add_argument(
        "--min-exit-basis-bps",
        dest="min_exit_basis_bps",
        type=float,
        required=True,
        help="Net basis floor (bps) for unwind. Captures convergence.",
    )
    parser_exit.add_argument(
        "--max-duration-s",
        dest="max_duration_s",
        type=float,
        required=True,
        help="Hard wall-clock deadline. Residual stays as a hedged position.",
    )

    # ABORT COMMAND (Signals graceful halt of an in-flight slicing loop)
    parser_abort = subparsers.add_parser("abort")
    parser_abort.add_argument(
        "--pair",
        required=True,
        help="Position key: the base coin of the active position (e.g. 'CHEEMS').",
    )

    args = parser.parse_args()

    payload = vars(args).copy()
    endpoint = payload.pop("action")

    send_command(endpoint, payload)
