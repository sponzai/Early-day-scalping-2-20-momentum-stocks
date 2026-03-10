import os
import pandas as pd
import yfinance as yf
from tqdm import tqdm

TICKER_FILE = "ross_universe_tickers.csv"
OUT_DIR = "data/daily_yahoo"
os.makedirs(OUT_DIR, exist_ok=True)

def load_all_tickers():
    df = pd.read_csv(TICKER_FILE, header=None, names=["ticker"])
    tickers = df["ticker"].tolist()
    return tickers

if __name__ == "__main__":
    tickers = load_all_tickers()
    print(f"Downloading 5-year daily data for {len(tickers)} tickers starting with A...")

    for ticker in tqdm(tickers):
        try:
            out_path = f"{OUT_DIR}/{ticker}.parquet"

            # Skip if already downloaded
            if os.path.exists(out_path):
                continue

            df = yf.download(ticker, period="5y", interval="1d", progress=False)

            if df.empty:
                continue

            df.reset_index().to_parquet(out_path, index=False)

        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
