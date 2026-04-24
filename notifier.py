import os
import requests
from dotenv import load_dotenv

load_dotenv()


def send_pushover(message, title="Cyborg Arb", priority=1, sound="CustomLongBellRing"):
    """Pushover integration for telemetry alerting."""
    data = {
        "token": os.environ.get("PUSHOVER_TOKEN"),
        "user": os.environ.get("PUSHOVER_USER"),
        "title": title,
        "message": message,
        "priority": priority,
        "sound": sound,
    }

    # Priority 2 requires retry/expire params to bypass DND
    if priority == 2:
        data["retry"] = 60
        data["expire"] = 3600

    try:
        resp = requests.post("https://api.pushover.net/1/messages.json", data=data)
        print(f"Pushover response: {resp.status_code} {resp.text}")
        return resp.ok
    except Exception as e:
        print(f"Pushover request failed: {e}")
        return False    
