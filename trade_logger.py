import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

LOG_FILE = os.getenv("TRADE_EVENT_LOG_FILE", "trade_events.csv")


def log_trade_event(
    event_type="EVENT",
    bot_name="unknown",
    symbol="",
    side="",
    qty="",
    price="",
    status="",
    message="",
    raw_payload=None,
    **kwargs
):
    timestamp = datetime.now(ZoneInfo("America/New_York")).isoformat()

    row = {
        "timestamp_et": timestamp,
        "event_type": event_type,
        "bot_name": bot_name,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "status": status,
        "message": message,
        "raw_payload": str(raw_payload) if raw_payload is not None else "",
    }

    for key, value in kwargs.items():
        row[key] = value

    file_exists = os.path.exists(LOG_FILE)

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
