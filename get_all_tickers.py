import pandas as pd

def load_all_us_tickers():
    print("Loading US tickers from NASDAQ...")

    urls = [
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    ]

    tickers = []

    for url in urls:
        df = pd.read_csv(url, sep="|")

        # Normalize column names
        if "Symbol" in df.columns:
            symbols = df["Symbol"]
        elif "ACT Symbol" in df.columns:
            symbols = df["ACT Symbol"]
        else:
            continue

        # Clean symbols
        symbols = symbols.dropna()
        symbols = symbols[symbols.str.isalpha()]  # remove weird symbols

        tickers.extend(symbols.tolist())

    tickers = sorted(set(tickers))  # remove duplicates
    print(f"Loaded {len(tickers)} tickers.")
    return tickers


if __name__ == "__main__":
    tickers = load_all_us_tickers()
    pd.Series(tickers).to_csv("all_us_tickers.csv", index=False)
    print("Saved all_us_tickers.csv")
