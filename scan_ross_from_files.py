import os
import pandas as pd
from tqdm import tqdm
import ast

DATA_DIR = "data/daily_yahoo"
OUT_DIR = "data/ross_hits"
os.makedirs(OUT_DIR, exist_ok=True)

# Ross Criteria (Adjusted to your target)
MIN_PRICE, MAX_PRICE = 1.0, 30.0
MIN_GAP, MIN_VOLUME, MIN_RVOL = 4.0, 100000, 1.5
RVOL_WINDOW = 20

def scan_yahoo_data():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
    print(f"Scanning {len(files)} Yahoo tickers with Tuple-Headers...")
    all_candidates = []

    for file in tqdm(files):
        ticker = file.replace(".parquet", "")
        try:
            df = pd.read_parquet(os.path.join(DATA_DIR, file))

            # --- STEP 1: CRACK THE TUPLE STRINGS ---
            # This turns "('Open', 'A')" into "Open"
            new_cols = []
            for col in df.columns:
                col_str = str(col)
                if '(' in col_str and ',' in col_str:
                    # Extract the first word inside the quotes
                    clean_name = col_str.split("'")[1]
                    new_cols.append(clean_name)
                else:
                    new_cols.append(col_str)
            df.columns = new_cols

            # --- STEP 2: DATE HANDLING ---
            if "Date" not in df.columns:
                df = df.reset_index()
            
            # Ensure Date is actually datetime objects
            df['Date'] = pd.to_datetime(df['Date'])
            
            # --- STEP 3: MOMENTUM MATH ---
            df = df.sort_values("Date").reset_index(drop=True)
            
            # Map for safety
            df = df.rename(columns={'Adj Close': 'Close'}) # Use Adj Close if available
            
            df['prev_close'] = df['Close'].shift(1)
            df['gap_pct'] = ((df['Open'] - df['prev_close']) / df['prev_close']) * 100
            df['avg_vol_20'] = df['Volume'].rolling(window=RVOL_WINDOW).mean().shift(1)
            df['rvol'] = df['Volume'] / df['avg_vol_20']

            # --- STEP 4: FILTER ---
            mask = (
                (df['prev_close'] >= MIN_PRICE) & (df['prev_close'] <= MAX_PRICE) &
                (df['gap_pct'] >= MIN_GAP) & 
                (df['Volume'] >= MIN_VOLUME) &
                (df['rvol'] >= MIN_RVOL)
            )

            hits = df[mask].copy()
            for _, row in hits.iterrows():
                date_str = row['Date'].strftime("%Y-%m-%d")
                
                # Check for 1970 again just in case
                if date_str.startswith("1970"): continue

                all_candidates.append({
                    "ticker": ticker, "date": date_str, 
                    "gap": round(row['gap_pct'], 2), 
                    "rvol": round(row['rvol'], 2), 
                    "price": round(row['prev_close'], 2)
                })
                # Save daily hit
                pd.DataFrame([row]).to_parquet(f"{OUT_DIR}/{ticker}_{date_str}.parquet", index=False)

        except Exception as e:
            # print(f"Error on {ticker}: {e}")
            continue

    pd.DataFrame(all_candidates).to_json("ross_master_to_do_list.json", orient="records", indent=4)
    print(f"\nScan complete! Found {len(all_candidates)} Ross-style candidates.")

if __name__ == "__main__":
    scan_yahoo_data()
