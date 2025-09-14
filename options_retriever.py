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
    
    # --- Individual snapshot calls (instead of batch) ---
    premiums = []
    valid_count = 0
    
    print("DEBUG: Fetching individual snapshots...")
    
    for ticker in ref_puts_df['ticker'].unique()[:20]:  # Limit to first 20 to avoid rate limits
        snapshot_url = f"https://api.polygon.io/v3/snapshot/options/{symbol}/{ticker}?apiKey={API_KEY}"
        try:
            resp = requests.get(snapshot_url)
            resp.raise_for_status()
            data = resp.json()
            results = data.get('results', {})
            
            # Extract premium data
            bid = results.get("bid")
            ask = results.get("ask")
            
            # Use close if available, fallback to bid
            premium_price = None
            if "day" in results and "close" in results["day"]:
                premium_price = results["day"]["close"]
            elif bid is not None:
                premium_price = bid
            
            # Compute spread if both bid & ask are available
            spread = None
            if bid is not None and ask is not None:
                spread = ask - bid
            
            if premium_price is not None:
                valid_count += 1
                
            premiums.append({
                "ticker": ticker,
                "premium": premium_price,
                "bid": bid,
                "ask": ask,
                "spread": spread
            })
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching snapshot for {ticker}: {e}")
            # Add empty record so we don't lose the ticker
            premiums.append({
                "ticker": ticker,
                "premium": None,
                "bid": None,
                "ask": None,
                "spread": None
            })
    
    print(f"DEBUG: {valid_count} individual snapshots have valid premium data")
    
    premium_df = pd.DataFrame(premiums)
    
    # --- Merge contract info + premium data ---
    full_put_df = pd.merge(ref_puts_df, premium_df, on="ticker", how="left")
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
    
    if len(full_put_df) > 0:
        sample_premiums = full_put_df["premium"].head(3).tolist()
        print(f"DEBUG: Sample premiums: {sample_premiums}")
    
    return full_put_df.to_dict(orient="records")