import pandas as pd

def load_tickers(csv_path="all_us_tickers.csv"):
    """
    Loads the full list of US tickers from your CSV file.
    Cleans them and returns a Python list.
    """
    df = pd.read_csv(csv_path, header=None, names=["ticker"])

    # Remove NaN, duplicates, and non‑alphabetic tickers
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].str.isalpha()]
    df = df.drop_duplicates()

    tickers = df["ticker"].tolist()
    print(f"Loaded {len(tickers)} tickers.")
    return tickers


if __name__ == "__main__":
    tickers = load_tickers()
    print(tickers[:50])  # preview first 50
