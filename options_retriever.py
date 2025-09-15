import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import concurrent.futures
import time

load_dotenv()
API_KEY = os.getenv("API_KEY")

def get_puts_for_ticker(symbol, upper_bound_strike, current_price, expiry, min_commission, max_spread):
    print(f"DEBUG: Starting search for {symbol}, current_price: {current_price}")
    
    # Add strike price filters to the API call to reduce data
    max_strike = current_price * (1 - upper_bound_strike / 100.0)
    print(f"max strike price {max_strike}")

    # Calculate expiry date range
    today = datetime.now()
    target_expiry = today + timedelta(days=expiry)
    min_expiry = target_expiry - timedelta(days=15)
    max_expiry = target_expiry + timedelta(days=15)

    ref_url = (f"https://api.polygon.io/v3/reference/options/contracts?"
               f"underlying_ticker={symbol}&contract_type=put&"
               f"strike_price.lte={max_strike}&"
               f"expiration_date.gte={min_expiry.strftime('%Y-%m-%d')}&"
               f"expiration_date.lte={max_expiry.strftime('%Y-%m-%d')}&"
               f"limit=1000&apiKey={API_KEY}")
    
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
            page_count += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break
    
    if not ref_put_list:
        print("DEBUG: No put contracts found!")
        return []
    
    print(f"DEBUG: Found {len(ref_put_list)} filtered put contracts")
    
    # Convert to DataFrame and apply additional filters
    ref_puts_df = pd.DataFrame(ref_put_list)
    ref_puts_df["expiration_date"] = pd.to_datetime(ref_puts_df["expiration_date"])
    
    ref_puts_df = ref_puts_df[
        (ref_puts_df["expiration_date"] >= min_expiry) &
        (ref_puts_df["expiration_date"] <= max_expiry) &
        (ref_puts_df["strike_price"] < current_price)
    ]
    
    print(f"DEBUG: After additional filtering: {len(ref_puts_df)} contracts")
    if ref_puts_df.empty:
        print("DEBUG: No contracts after filtering!")
        return []

    # Sort by expiration and strike
    ref_puts_df = ref_puts_df.sort_values(['expiration_date', 'strike_price'], ascending=[True, False])

    # --- Parallel API calls for snapshots ---
    def fetch_snapshot(ticker):
        snapshot_url = f"https://api.polygon.io/v3/snapshot/options/{symbol}/{ticker}?apiKey={API_KEY}"
        try:
            resp = requests.get(snapshot_url)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", {})

            bid = results.get("bid")
            ask = results.get("ask")

            # Only use liquid options with both bid and ask
            if bid is not None and ask is not None:
                premium_price = (bid + ask) / 2  # midpoint for realistic premium
                spread = ask - bid
            else:
                premium_price = None
                spread = None

            return {
                "ticker": ticker,
                "premium": premium_price,
                "bid": bid,
                "ask": ask,
                "spread": spread
            }
        except Exception as e:
            print(f"Error fetching snapshot for {ticker}: {e}")
            return {
                "ticker": ticker,
                "premium": None,
                "bid": None,
                "ask": None,
                "spread": None
            }

    # Fetch snapshots in parallel
    premiums = []
    tickers = ref_puts_df['ticker'].tolist()
    
    print("DEBUG: Fetching snapshots in parallel...")
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_ticker = {executor.submit(fetch_snapshot, ticker): ticker for ticker in tickers}
        for future in concurrent.futures.as_completed(future_to_ticker):
            premiums.append(future.result())
    
    end_time = time.time()
    print(f"DEBUG: Fetched {len(premiums)} snapshots in {end_time - start_time:.2f} seconds")

    # Only keep options with valid midpoint premium
    premium_df = pd.DataFrame(premiums)
    premium_df = premium_df[premium_df["premium"].notna()]
    print(f"DEBUG: {len(premium_df)} snapshots have valid premium data")

    # Merge with reference contracts
    full_put_df = pd.merge(ref_puts_df, premium_df, on="ticker", how="inner")
    full_put_df.dropna(subset=["premium"], inplace=True)
    print(f"DEBUG: After merge and dropping nulls: {len(full_put_df)} contracts")

    # Final filters
    full_put_df = full_put_df[
        (full_put_df["premium"] >= min_commission) &
        ((full_put_df["spread"].isna()) | (full_put_df["spread"] <= max_spread))
    ]
    print(f"DEBUG: After final filter: {len(full_put_df)} contracts")

    if len(full_put_df) > 0:
        sample_data = full_put_df[['strike_price', 'premium', 'expiration_date']].head(3)
        print(f"DEBUG: Sample results:\n{sample_data}")

    return full_put_df.to_dict(orient="records")
