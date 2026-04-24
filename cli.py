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
            res_body = response.read().decode("utf-8")
            res_data = json.loads(res_body)
            if response.status == 200:
                print(f"✅ SUCCESS: {res_data.get('message')}")
            else:
                print(f"❌ ERROR: {res_data.get('error')}")

    except urllib.error.HTTPError as e:
        res_body = e.read().decode("utf-8")
        res_data = json.loads(res_body) if res_body else {}
        print(f"❌ ERROR: {res_data.get('error', e.reason)}")
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

    # ENTRY COMMAND (Network Light - Instant Execution with explicit routing)
    parser_entry = subparsers.add_parser("entry")
    parser_entry.add_argument("--symbol", required=True)
    parser_entry.add_argument("--long", required=True)
    parser_entry.add_argument("--short", required=True)
    parser_entry.add_argument(
        "--amount", type=float, required=True, help="Exact base token amount"
    )

    # EXIT COMMAND (Network Light - Instant Execution via state lookup)
    parser_exit = subparsers.add_parser("exit")
    parser_exit.add_argument("--symbol", required=True)

    args = parser.parse_args()

    payload = vars(args).copy()
    endpoint = payload.pop("action")

    send_command(endpoint, payload)
