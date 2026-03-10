import pandas as pd
import os

INPUT_FILE = "all_us_tickers.csv"
OUTPUT_FILE = "ross_universe_tickers.csv"

def load_raw_tickers():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found!")
        return []
    
    # Read CSV - assuming no header as per your previous script
    df = pd.read_csv(INPUT_FILE, header=None, names=["ticker"])
    
    # 1. Clean formatting (strip spaces and uppercase)
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    
    # 2. REMOVE DOTS/DASHES instead of deleting the whole ticker
    # This keeps BRK.B by turning it into BRKB
    df["ticker"] = df["ticker"].str.replace(r'[^A-Z]', '', regex=True)
    
    # 3. Drop empty or invalid
    df = df[df["ticker"] != ""]
    
    raw_list = df["ticker"].drop_duplicates().tolist()
    print(f"Successfully loaded {len(raw_list)} unique tickers from file.")
    return raw_list

def filter_ross_universe(tickers):
    final = []
    # Structural suffixes to exclude
    STRUCTURAL_SUFFIXES = ("W", "WS", "WT", "U", "UT", "R", "RT", "P", "PR", "Q")

    for t in tickers:
        # Rule 1: Remove Suffixes
        if any(t.endswith(suf) for suf in STRUCTURAL_SUFFIXES):
            continue

        # Rule 2: Remove 5-letter Bankruptcy/ADR (Ending in Q, Y, F)
        if len(t) == 5 and t[-1] in ("Q", "Y", "F"):
            continue

        # Rule 3: Max length (Ross-style is 3-4 letters, rarely 5)
        if len(t) > 5:
            continue

        final.append(t)
    
    print(f"Filtered down to {len(final)} Ross-style candidates.")
    return sorted(set(final))

if __name__ == "__main__":
    raw = load_raw_tickers()
    if raw:
        clean = filter_ross_universe(raw)
        pd.Series(clean).to_csv(OUTPUT_FILE, index=False, header=False)
        print(f"Saved to {OUTPUT_FILE}")
