import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")


def get_puts_for_ticker(symbol, upper_bound_strike, current_price, expiry, min_commission, max_spread):
    # --- Get contract metadata ---
    ref_url = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={symbol}&contract_type=put&apiKey={API_KEY}"

    ref_put_list = []
    while ref_url:
        try:
            resp = requests.get(ref_url)
            resp.raise_for_status()
            data = resp.json()

            ref_put_list.extend(data.get("results", []))
            ref_url = data.get("next_url")

            if ref_url and "apiKey=" not in ref_url:
                ref_url = f"{ref_url}&apiKey={API_KEY}"

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    if not ref_put_list:
        return []

    ref_puts_df = pd.DataFrame(ref_put_list)
    ref_puts_df["expiration_date"] = pd.to_datetime(ref_puts_df["expiration_date"])

    # --- Expiry filter (±15 days of target expiry) ---
    today = datetime.now()
    target_expiry = today + timedelta(days=expiry)
    min_expiry = target_expiry - timedelta(days=15)
    max_expiry = target_expiry + timedelta(days=15)
    ref_puts_df = ref_puts_df[
        (ref_puts_df["expiration_date"] >= min_expiry)
        & (ref_puts_df["expiration_date"] <= max_expiry)
    ]

    # --- Strike price filter ---
    lower_bound = current_price * (1 - upper_bound_strike / 100.0)
    ref_puts_df = ref_puts_df[
        (ref_puts_df["strike_price"] < current_price)
        & (ref_puts_df["strike_price"] >= lower_bound)
    ]

    if ref_puts_df.empty:
        return []

    # --- Batch snapshot (all options at once) ---
    snapshot_url = f"https://api.polygon.io/v3/snapshot/options/{symbol}?apiKey={API_KEY}"
    try:
        resp = requests.get(snapshot_url)
        resp.raise_for_status()
        snapshots = resp.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching snapshot batch: {e}")
        return []

    # Build DataFrame of premiums
    premiums = []
    for snap in snapshots:
        ticker = snap.get("ticker")
        details = snap.get("details", {})
        day_data = snap.get("day", {})

        bid = details.get("bid")
        ask = details.get("ask")

        # Prefer close price, then bid
        premium_price = day_data.get("close") if "close" in day_data else bid

        spread = ask - bid if (bid is not None and ask is not None) else None

        premiums.append(
            {
                "ticker": ticker,
                "premium": premium_price,
                "bid": bid,
                "ask": ask,
                "spread": spread,
            }
        )

    premium_df = pd.DataFrame(premiums)

    # --- Merge contract info + premium data ---
    full_put_df = pd.merge(ref_puts_df, premium_df, on="ticker", how="inner")
    full_put_df.dropna(subset=["premium"], inplace=True)

    # --- Commission + Spread filter ---
    full_put_df = full_put_df[
        (full_put_df["premium"] >= min_commission)
        & ((full_put_df["spread"].isna()) | (full_put_df["spread"] <= max_spread))
    ]

    return full_put_df.to_dict(orient="records")
