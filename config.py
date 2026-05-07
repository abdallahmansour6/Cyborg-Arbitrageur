import os
import ccxt.pro as ccxtpro
from dotenv import load_dotenv

load_dotenv()


def get_exchange(exchange_id):
    """Instantiates CCXT Pro instances for the Engine."""
    creds = {
        "apiKey": os.getenv(f"{exchange_id.upper()}_API_KEY"),
        "secret": os.getenv(f"{exchange_id.upper()}_SECRET"),
        "enableRateLimit": False,  # In this context, we will not let the library throttle us, we blast the network instantly; if we hit a limit, we let the exchange reject us.
        "options": {"defaultType": "swap"},  # Strictly perps
        "newUpdates": True,  # WebSocket order book deltas for max speed and minimal bandwidth
    }

    # Catch API Passphrases required by OKX, KuCoin, Bitget, etc.
    password = os.getenv(f"{exchange_id.upper()}_PASSWORD")
    if password:
        creds["password"] = password
    
    exchange_class = getattr(ccxtpro, exchange_id.lower())
    return exchange_class(creds)
