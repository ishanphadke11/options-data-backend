from flask import Flask, jsonify, request
from flask_cors import CORS
from options_retriever import get_puts_for_ticker
import yfinance as yf
import os

app = Flask(__name__)
CORS(app)

@app.route("/options/<symbol>", methods=["GET"])
def get_options(symbol):
    try:
        upper_bound = float(request.args.get("upper_bound", 8))
        expiry = int(request.args.get("expiry", 30))
        min_commission = float(request.args.get("min_commission", 1.0))
        max_spread = float(request.args.get("max_spread", 0.5))
        
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if hist.empty:
            return jsonify({"error": "No price found for symbol"}), 404
            
        current_price = hist["Close"].iloc[-1]
        
        results = get_puts_for_ticker(
            symbol=symbol,
            upper_bound_strike=upper_bound,
            expiry=expiry,
            current_price=current_price,
            min_commision=min_commission,  # Note: typo preserved from original
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