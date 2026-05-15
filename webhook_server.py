import csv
import os
from datetime import datetime, time
from zoneinfo import ZoneInfo
import json
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

from dotenv import load_dotenv
from flask import Flask, jsonify, request

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, TakeProfitRequest, StopLossRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").lower() == "true"

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "coachsq_secret_123")
DATABASE_URL = os.getenv("DATABASE_URL")
db_engine = create_engine(DATABASE_URL) if DATABASE_URL else None

print(f"DATABASE_URL loaded: {'YES' if DATABASE_URL else 'NO'}")
print(f"Database engine ready: {'YES' if db_engine else 'NO'}")

ALLOWED_SYMBOLS = {
    s.strip().upper()
    for s in os.getenv("BOT_ALLOWED_SYMBOLS", "AMD,TSLA,QQQ").split(",")
    if s.strip()
}

MAX_TOTAL_TRADES_PER_DAY = int(os.getenv("BOT_MAX_TOTAL_TRADES_PER_DAY", "6"))
MAX_TRADES_PER_SYMBOL_PER_DAY = int(os.getenv("BOT_MAX_TRADES_PER_SYMBOL_PER_DAY", "2"))
RISK_DOLLARS_PER_TRADE = float(os.getenv("BOT_RISK_DOLLARS_PER_TRADE", "2"))
MAX_SHARES_PER_TRADE = int(os.getenv("BOT_MAX_SHARES_PER_TRADE", "5"))
USE_LIVE_ENTRY_PRICE = os.getenv("BOT_USE_LIVE_ENTRY_PRICE", "true").lower() == "true"
AUTO_BRACKET = os.getenv("BOT_AUTO_BRACKET", "true").lower() == "true"
AUTO_STOP_DOLLARS = float(os.getenv("BOT_AUTO_STOP_DOLLARS", "1.50"))
AUTO_TARGET_DOLLARS = float(os.getenv("BOT_AUTO_TARGET_DOLLARS", "3.00"))

LOG_FILE = "trade_log.csv"
EASTERN = ZoneInfo("America/New_York")

# ==============================
# Trading Window Protection
# ==============================

ALLOW_NEW_TRADES_AFTER = time(9, 45)
STOP_NEW_TRADES_AFTER = time(15, 30)

def trading_time_allowed():
    now = datetime.now(EASTERN).time()

    if now < ALLOW_NEW_TRADES_AFTER:
        return False, "Trading blocked — before 9:45 AM ET"

    if now >= STOP_NEW_TRADES_AFTER:
        return False, "Trading blocked — after 3:30 PM ET"

    return True, "Trading window approved"

app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return {
        "ok": True,
        "bot": "TradingView Bot - QQQ TSLA AMD",
        "status": "online"
    }, 200

@app.route("/db-test", methods=["GET"])
def db_test():
    if not DATABASE_URL:
        return {
            "ok": False,
            "error": "DATABASE_URL is not loaded",
            "database_url_loaded": False,
        }, 500

    try:
        parsed = urlparse(DATABASE_URL)
        db_host = parsed.hostname

        if db_engine is None:
            return {
                "ok": False,
                "error": "db_engine is None",
                "database_url_loaded": True,
                "db_host": db_host,
            }, 500

        with db_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO bot_events (
                        bot_name,
                        event_type,
                        symbol,
                        side,
                        strategy,
                        model,
                        status,
                        message
                    )
                    VALUES (
                        'TradingView Bot - QQQ TSLA AMD',
                        'DB_TEST',
                        'AMD',
                        'buy',
                        'database_test',
                        'railway_db_test',
                        'SUCCESS',
                        'Manual DB test inserted from Railway TradingView bot'
                    )
                """)
            )

        return {
            "ok": True,
            "database_url_loaded": True,
            "db_host": db_host,
            "message": "DB test insert successful",
        }, 200

    except Exception as e:
        parsed = urlparse(DATABASE_URL)
        return {
            "ok": False,
            "database_url_loaded": True,
            "db_host": parsed.hostname,
            "error": str(e),
        }, 500

trading_client = TradingClient(
    api_key=ALPACA_API_KEY,
    secret_key=ALPACA_SECRET_KEY,
    paper=ALPACA_PAPER,
)

data_client = StockHistoricalDataClient(
    api_key=ALPACA_API_KEY,
    secret_key=ALPACA_SECRET_KEY,
)


def now_et():
    return datetime.now(EASTERN)


def today():
    return now_et().strftime("%Y-%m-%d")


def ensure_log():
    if os.path.exists(LOG_FILE):
        return

    with open(LOG_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_et",
            "date_et",
            "symbol",
            "side",
            "entry",
            "stop_loss",
            "take_profit",
            "qty",
            "status",
            "reason",
            "raw_payload",
        ])


def log_event(symbol, side, entry, stop_loss, take_profit, qty, status, reason, payload):
    ensure_log()

    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            now_et().strftime("%Y-%m-%d %H:%M:%S"),
            today(),
            symbol,
            side,
            entry,
            stop_loss,
            take_profit,
            qty,
            status,
            reason,
            str(payload),
        ])


def accepted_trades_today(symbol=None):
    ensure_log()
    count = 0

    with open(LOG_FILE, "r", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row.get("date_et") != today():
                continue

            if row.get("status") != "ACCEPTED":
                continue

            if symbol and row.get("symbol") != symbol:
                continue

            count += 1

    return count


def has_open_position(symbol):
    try:
        positions = trading_client.get_all_positions()
        return any(p.symbol.upper() == symbol.upper() for p in positions)
    except Exception:
        return False


def get_float(payload, key):
    if key not in payload:
        raise ValueError(f"Missing field: {key}")
    return float(payload[key])


def get_side(side_text):
    side_text = str(side_text).lower().strip()

    if side_text in ["buy", "long"]:
        return OrderSide.BUY

    if side_text in ["sell", "short"]:
        return OrderSide.SELL

    raise ValueError(f"Invalid side: {side_text}")


def calculate_qty(entry, stop_loss):
    risk_per_share = abs(entry - stop_loss)

    if risk_per_share <= 0:
        raise ValueError("Invalid risk per share.")

    qty = int(RISK_DOLLARS_PER_TRADE // risk_per_share)

    if qty < 1:
        raise ValueError(
            f"Risk per share ${risk_per_share:.2f} is too high for "
            f"${RISK_DOLLARS_PER_TRADE:.2f} risk."
        )

    qty = min(qty, MAX_SHARES_PER_TRADE)
    return qty

def get_live_price(symbol):
    try:
        request_params = StockLatestTradeRequest(symbol_or_symbols=symbol)
        latest_trade = data_client.get_stock_latest_trade(request_params)

        if isinstance(latest_trade, dict):
            trade = latest_trade.get(symbol)
        else:
            trade = latest_trade

        if trade is None:
            raise ValueError(f"No latest trade found for {symbol}")

        return float(trade.price)

    except Exception as e:
        raise ValueError(f"Could not get live price for {symbol}: {e}")


def validate_payload(payload):
    secret = str(payload.get("secret", ""))

    if secret != WEBHOOK_SECRET:
        raise ValueError("Invalid webhook secret.")

    symbol = str(payload.get("symbol", "")).upper().strip()

    if symbol not in ALLOWED_SYMBOLS:
        raise ValueError(f"Symbol not allowed: {symbol}")

    side_text = str(payload.get("side", "")).lower().strip()
    side = get_side(side_text)

    if USE_LIVE_ENTRY_PRICE:
        entry = round(get_live_price(symbol), 2)
    else:
        entry = get_float(payload, "entry")

    if AUTO_BRACKET:
        if side == OrderSide.BUY:
            stop_loss = round(entry - AUTO_STOP_DOLLARS, 2)
            take_profit = round(entry + AUTO_TARGET_DOLLARS, 2)
        elif side == OrderSide.SELL:
            stop_loss = round(entry + AUTO_STOP_DOLLARS, 2)
            take_profit = round(entry - AUTO_TARGET_DOLLARS, 2)
    else:
        stop_loss = get_float(payload, "stop_loss")
        take_profit = get_float(payload, "take_profit")

    if side == OrderSide.BUY and not (stop_loss < entry < take_profit):
        raise ValueError("Invalid buy setup. Need stop_loss < entry < take_profit.")

    if side == OrderSide.SELL and not (take_profit < entry < stop_loss):
        raise ValueError("Invalid sell setup. Need take_profit < entry < stop_loss.")

    qty = calculate_qty(entry, stop_loss)

    return symbol, side_text, side, entry, stop_loss, take_profit, qty


def submit_bracket_order(symbol, side, qty, stop_loss, take_profit):
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=side,
        type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.BRACKET,
        take_profit=TakeProfitRequest(limit_price=round(take_profit, 2)),
        stop_loss=StopLossRequest(stop_price=round(stop_loss, 2)),
    )

    return trading_client.submit_order(order_data=order)


@app.get("/")
def home():
    return jsonify({
        "status": "running",
        "paper": ALPACA_PAPER,
        "allowed_symbols": sorted(ALLOWED_SYMBOLS),
        "risk_dollars_per_trade": RISK_DOLLARS_PER_TRADE,
        "max_shares_per_trade": MAX_SHARES_PER_TRADE,
        "max_total_trades_per_day": MAX_TOTAL_TRADES_PER_DAY,
        "max_trades_per_symbol_per_day": MAX_TRADES_PER_SYMBOL_PER_DAY,
        "accepted_trades_today": accepted_trades_today(),
    })

def log_db_event(
    event_type,
    symbol=None,
    side=None,
    strategy=None,
    model=None,
    status=None,
    qty=None,
    entry=None,
    stop_loss=None,
    take_profit=None,
    order_id=None,
    message=None,
    raw_payload=None,
):
    if db_engine is None:
        print("Database log skipped: db_engine is None")
        return

    try:
        payload_text = None
        if raw_payload is not None:
            payload_text = json.dumps(raw_payload)

        with db_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO bot_events (
                        bot_name, event_type, symbol, side, strategy, model,
                        status, qty, entry, stop_loss, take_profit,
                        order_id, message, raw_payload
                    )
                    VALUES (
                        :bot_name, :event_type, :symbol, :side, :strategy, :model,
                        :status, :qty, :entry, :stop_loss, :take_profit,
                        :order_id, :message, :raw_payload
                    )
                """),
                {
                    "bot_name": "TradingView Bot - QQQ TSLA AMD",
                    "event_type": event_type,
                    "symbol": symbol,
                    "side": side,
                    "strategy": strategy,
                    "model": model,
                    "status": status,
                    "qty": qty,
                    "entry": entry,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "order_id": order_id,
                    "message": message,
                    "raw_payload": payload_text,
                },
            )
    except Exception as e:
        print(f"Database log failed: {e}", flush=True)


@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True)

    if not isinstance(payload, dict):
        log_db_event(
            event_type="WEBHOOK_REJECTED",
            status="REJECTED",
            message="Invalid JSON payload",
            raw_payload=payload,
        )
        return jsonify({"ok": False, "error": "Invalid JSON payload"}), 400

    symbol = str(payload.get("symbol", "")).upper()
    side_text = str(payload.get("side", "")).lower()
    strategy = payload.get("strategy")
    model = payload.get("model")

    log_db_event(
        event_type="WEBHOOK_RECEIVED",
        symbol=symbol,
        side=side_text,
        strategy=strategy,
        model=model,
        status="RECEIVED",
        message="TradingView webhook received",
        raw_payload=payload,
    )

    try:
        symbol, side_text, side, entry, stop_loss, take_profit, qty = validate_payload(payload)

        time_ok, time_reason = trading_time_allowed()

        if not time_ok:
            log_event(symbol, side_text, entry, stop_loss, take_profit, qty, "REJECTED", time_reason, payload)

            log_db_event(
                event_type="TRADE_REJECTED",
                symbol=symbol,
                side=side_text,
                strategy=strategy,
                model=model,
                status="REJECTED",
                qty=qty,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                message=time_reason,
                raw_payload=payload,
            )

            return jsonify({"ok": False, "rejected": time_reason}), 200

        if accepted_trades_today() >= MAX_TOTAL_TRADES_PER_DAY:
            reason = "Max total trades per day reached."

            log_event(symbol, side_text, entry, stop_loss, take_profit, qty, "REJECTED", reason, payload)

            log_db_event(
                event_type="TRADE_REJECTED",
                symbol=symbol,
                side=side_text,
                strategy=strategy,
                model=model,
                status="REJECTED",
                qty=qty,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                message=reason,
                raw_payload=payload,
            )

            return jsonify({"ok": False, "rejected": reason}), 200

        if accepted_trades_today(symbol) >= MAX_TRADES_PER_SYMBOL_PER_DAY:
            reason = f"Max trades reached today for {symbol}."

            log_event(symbol, side_text, entry, stop_loss, take_profit, qty, "REJECTED", reason, payload)

            log_db_event(
                event_type="TRADE_REJECTED",
                symbol=symbol,
                side=side_text,
                strategy=strategy,
                model=model,
                status="REJECTED",
                qty=qty,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                message=reason,
                raw_payload=payload,
            )

            return jsonify({"ok": False, "rejected": reason}), 200

        if has_open_position(symbol):
            reason = f"Already in open position for {symbol}."

            log_event(symbol, side_text, entry, stop_loss, take_profit, qty, "REJECTED", reason, payload)

            log_db_event(
                event_type="TRADE_REJECTED",
                symbol=symbol,
                side=side_text,
                strategy=strategy,
                model=model,
                status="REJECTED",
                qty=qty,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                message=reason,
                raw_payload=payload,
            )

            return jsonify({"ok": False, "rejected": reason}), 200

        order = submit_bracket_order(symbol, side, qty, stop_loss, take_profit)

        reason = f"Submitted Alpaca paper bracket order: {order.id}"

        log_event(symbol, side_text, entry, stop_loss, take_profit, qty, "ACCEPTED", reason, payload)

        log_db_event(
            event_type="TRADE_PLACED",
            symbol=symbol,
            side=side_text,
            strategy=strategy,
            model=model,
            status="ACCEPTED",
            qty=qty,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            order_id=str(order.id),
            message=reason,
            raw_payload=payload,
        )

        return jsonify({
            "ok": True,
            "symbol": symbol,
            "side": side_text,
            "qty": qty,
            "entry_from_alert": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "order_id": str(order.id),
        }), 200

    except Exception as e:
        reason = str(e)

        log_event(symbol, side_text, "", "", "", "", "REJECTED", reason, payload)

        log_db_event(
            event_type="ERROR",
            symbol=symbol,
            side=side_text,
            strategy=strategy,
            model=model,
            status="ERROR",
            message=reason,
            raw_payload=payload,
        )

        return jsonify({"ok": False, "error": reason}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)