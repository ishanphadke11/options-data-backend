import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

def get_puts_for_ticker(symbol, upper_bound_strike, current_price, expiry, min_commission, max_spread):
    print(f"DEBUG: Starting search for {symbol}, current_price: {current_price}")
    
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
        print("DEBUG: No put contracts found!")
        return []
    
    print(f"DEBUG: Found {len(ref_put_list)} total put contracts")
    
    ref_puts_df = pd.DataFrame(ref_put_list)
    ref_puts_df["expiration_date"] = pd.to_datetime(ref_puts_df["expiration_date"])
    
    # --- Expiry filter (±15 days of target expiry) ---
    today = datetime.now()
    target_expiry = today + timedelta(days=expiry)
    min_expiry = target_expiry - timedelta(days=15)
    max_expiry = target_expiry + timedelta(days=15)
    
    print(f"DEBUG: Expiry filter - looking for dates between {min_expiry.date()} and {max_expiry.date()}")
    
    ref_puts_df = ref_puts_df[
        (ref_puts_df["expiration_date"] >= min_expiry)
        & (ref_puts_df["expiration_date"] <= max_expiry)
    ]
    
    print(f"DEBUG: After expiry filter: {len(ref_puts_df)} contracts")
    
    # --- Strike price filter ---
    lower_bound = current_price * (1 - upper_bound_strike / 100.0)
    print(f"DEBUG: Strike filter - looking for strikes between {lower_bound:.2f} and {current_price:.2f}")
    
    ref_puts_df = ref_puts_df[
        (ref_puts_df["strike_price"] < current_price)
        & (ref_puts_df["strike_price"] >= lower_bound)
    ]
    
    print(f"DEBUG: After strike filter: {len(ref_puts_df)} contracts")
    
    if ref_puts_df.empty:
        print("DEBUG: No contracts after filtering!")
        return []
    
    # Show some sample strikes for debugging
    if len(ref_puts_df) > 0:
        sample_strikes = ref_puts_df["strike_price"].head(5).tolist()
        print(f"DEBUG: Sample strikes: {sample_strikes}")
    
    # --- Batch snapshot (all options at once) ---
    snapshot_url = f"https://api.polygon.io/v3/snapshot/options/{symbol}?apiKey={API_KEY}"
    try:
        resp = requests.get(snapshot_url)
        resp.raise_for_status()
        snapshots = resp.json().get("results", [])
        print(f"DEBUG: Got {len(snapshots)} snapshots from API")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching snapshot batch: {e}")
        return []
    
    # Build DataFrame of premiums
    premiums = []
    valid_premiums = 0
    
    for snap in snapshots:
        ticker = snap.get("ticker")
        details = snap.get("details", {})
        day_data = snap.get("day", {})
        
        bid = details.get("bid")
        ask = details.get("ask")
        
        # Prefer close price, then bid
        premium_price = day_data.get("close") if "close" in day_data else bid
        spread = ask - bid if (bid is not None and ask is not None) else None
        
        if premium_price is not None:
            valid_premiums += 1
        
        premiums.append({
            "ticker": ticker,
            "premium": premium_price,
            "bid": bid,
            "ask": ask,
            "spread": spread,
        })
    
    print(f"DEBUG: {valid_premiums} snapshots have valid premium data")
    
    premium_df = pd.DataFrame(premiums)
    
    # --- Merge contract info + premium data ---
    print(f"DEBUG: Merging {len(ref_puts_df)} contracts with {len(premium_df)} premiums")
    
    # Check if tickers match before merge
    contract_tickers = set(ref_puts_df["ticker"].tolist())
    premium_tickers = set(premium_df["ticker"].tolist())
    matching_tickers = contract_tickers.intersection(premium_tickers)
    print(f"DEBUG: {len(matching_tickers)} matching tickers between contracts and premiums")
    
    if len(matching_tickers) == 0:
        print("DEBUG: No matching tickers! Sample contract tickers:", list(contract_tickers)[:3])
        print("DEBUG: Sample premium tickers:", list(premium_tickers)[:3])
    
    full_put_df = pd.merge(ref_puts_df, premium_df, on="ticker", how="inner")
    print(f"DEBUG: After merge: {len(full_put_df)} contracts")
    
    full_put_df.dropna(subset=["premium"], inplace=True)
    print(f"DEBUG: After dropping null premiums: {len(full_put_df)} contracts")
    
    # --- Commission + Spread filter ---
    before_final_filter = len(full_put_df)
    full_put_df = full_put_df[
        (full_put_df["premium"] >= min_commission)
        & ((full_put_df["spread"].isna()) | (full_put_df["spread"] <= max_spread))
    ]
    
    print(f"DEBUG: After final filter (commission >= {min_commission}, spread <= {max_spread}): {len(full_put_df)} contracts")
    print(f"DEBUG: Commission filter removed: {before_final_filter - len(full_put_df)} contracts")
    
    if len(full_put_df) > 0:
        sample_premiums = full_put_df["premium"].head(3).tolist()
        print(f"DEBUG: Sample premiums: {sample_premiums}")
    
    return full_put_df.to_dict(orient="records")