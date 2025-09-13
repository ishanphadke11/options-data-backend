import time
import yfinance as yf
from flask import Flask, jsonify, request
from flask_cors import CORS
from options_retriever import get_puts_for_ticker
import os

app = Flask(__name__)
CORS(app)

def get_current_price(symbol, max_retries=5, delay=2):
    """Try multiple times to fetch the current price from Yahoo Finance."""
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                return hist["Close"].iloc[-1]
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {symbol}: {e}")
        time.sleep(delay)  # wait before retrying
    return None  # after all retries failed

@app.route("/options/<symbol>", methods=["GET"])
def get_options(symbol):
    try:
        upper_bound = float(request.args.get("upper_bound", 8))
        expiry = int(request.args.get("expiry", 30))
        min_commission = float(request.args.get("min_commission", 1.0))
        max_spread = float(request.args.get("max_spread", 0.5))

        current_price = get_current_price(symbol)
        if current_price is None:
            return jsonify({"error": f"No price found for symbol {symbol}"}), 404

        results = get_puts_for_ticker(
            symbol=symbol,
            upper_bound_strike=upper_bound,
            expiry=expiry,
            current_price=current_price,
            min_commision=min_commission,  # keep the typo if your function expects it
            max_spread=max_spread
        )

        return jsonify({
            "stock_price": round(current_price, 2),
            "options": results
        })

    except Exception as e:
        print("Error in backend:", e)
        return jsonify({"error": str(e)}), 400


@app.route("/favicon.ico")
def favicon():
    return "", 204  # No Content

@app.route("/")
def root():
    return jsonify({"message": "Flask backend is running!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)