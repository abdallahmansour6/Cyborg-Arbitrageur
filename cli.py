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


def _render_success(res_data):
    """One-line ack from any Engine success payload.
    warmup/abort return {message}; entry/exit return {filled, target, halt_reason, [remaining]}.
    """
    if "message" in res_data:
        return res_data["message"]
    parts = []
    if "filled" in res_data:
        parts.append(f"filled={res_data['filled']:.8f}/{res_data.get('target', '?')}")
    if "remaining" in res_data:
        parts.append(f"remaining={res_data['remaining']:.8f}")
    if "halt_reason" in res_data:
        parts.append(f"halt={res_data['halt_reason']}")
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
    parser_warmup = subparsers.add_parser("warmup")
    parser_warmup.add_argument("--symbol", required=True)
    parser_warmup.add_argument(
        "--exchanges",
        nargs="+",
        required=True,
        help="List of exchanges e.g. binance bybit okx",
    )
    parser_warmup.add_argument("--leverage", type=int, required=True)

    # ENTRY COMMAND (Drives Synchronized Smart Slicing on the Engine)
    parser_entry = subparsers.add_parser("entry")
    parser_entry.add_argument("--symbol", required=True)
    parser_entry.add_argument("--long", required=True)
    parser_entry.add_argument("--short", required=True)
    parser_entry.add_argument(
        "--amount", type=float, required=True, help="Target base token amount"
    )
    parser_entry.add_argument(
        "--min-entry-basis-bps",
        type=float,
        required=True,
        help="Net basis floor (bps). Slices below this are not dispatched.",
    )
    parser_entry.add_argument(
        "--max-duration-s",
        type=float,
        required=True,
        help="Hard wall-clock deadline. Partial fills are kept as a hedged position.",
    )

    # EXIT COMMAND (Drives reduceOnly Synchronized Smart Slicing on the Engine)
    parser_exit = subparsers.add_parser("exit")
    parser_exit.add_argument("--symbol", required=True)
    parser_exit.add_argument(
        "--amount", type=float, required=True, help="Base token amount to unwind"
    )
    parser_exit.add_argument(
        "--min-exit-basis-bps",
        type=float,
        required=True,
        help="Net basis floor (bps) for unwind. Captures convergence.",
    )
    parser_exit.add_argument(
        "--max-duration-s",
        type=float,
        required=True,
        help="Hard wall-clock deadline. Residual stays as a hedged position.",
    )

    # ABORT COMMAND (Signals graceful halt of an in-flight slicing loop)
    parser_abort = subparsers.add_parser("abort")
    parser_abort.add_argument("--symbol", required=True)

    args = parser.parse_args()

    payload = vars(args).copy()
    endpoint = payload.pop("action")

    send_command(endpoint, payload)
