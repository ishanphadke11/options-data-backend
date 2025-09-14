import os
import time
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from options_retriever import get_puts_for_ticker
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

app = Flask(__name__)
CORS(app)

def get_current_price(symbol, max_retries=5, delay=2):
    """Fetch current price from Finnhub with retries."""
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol, "token": FINNHUB_API_KEY}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            if "c" in data and data["c"] > 0:
                return data["c"]
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {symbol}: {e}")
            time.sleep(delay)
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
            min_commission=min_commission,  # keep the typo if your function expects it
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
