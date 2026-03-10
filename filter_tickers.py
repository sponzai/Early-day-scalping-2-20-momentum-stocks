import pandas as pd
import re

INPUT_FILE = "filtered_tickers.csv"
OUTPUT_FILE = "filtered_tickers_v2.csv"

def load_tickers():
    df = pd.read_csv(INPUT_FILE, header=None, names=["ticker"])
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df[df["ticker"].str.isalpha()]
    df = df.drop_duplicates()
    return df["ticker"].tolist()

def filter_v2(tickers):
    final = []

    # Suffixes to exclude
    EXCLUDE_SUFFIXES = (
        "W", "WS", "WT", "U", "UT", "R", "RT", "P", "PR", "Q", "F", "Y", "V"
    )

    # ETF / ETN / Fund keywords
    ETF_KEYWORDS = (
        "ETF", "ETN", "FUND", "TRUST", "INDEX", "SPDR", "ISHARES",
        "ULTRA", "PROSHARES", "INVERSE", "LEVERAGED", "BOND", "NOTE",
        "INCOME", "DIVIDEND"
    )

    # Known ETF tickers (no keywords)
    KNOWN_ETFS = {
        "SPY","QQQ","IWM","DIA","VOO","VTI","XLF","XLE","XLI","XLB","XLY","XLP",
        "XLV","XLU","XBI","SMH","SOXL","SOXS","TQQQ","SQQQ","UVXY","SVXY",
        "LABU","LABD","ARKK","ARKG","ARKF","ARKW","ARKQ"
    }

    # Index-like patterns
    INDEX_PATTERNS = ("500", "1000", "2000", "3000", "VIX", "VOL")

    for t in tickers:

        # Remove known ETFs
        if t in KNOWN_ETFS:
            continue

        # Remove ETF-like keywords
        if any(k in t for k in ETF_KEYWORDS):
            continue

        # Remove index-like tickers
        if any(p in t for p in INDEX_PATTERNS):
            continue

        # Remove suffix-based structural tickers
        if any(t.endswith(suf) for suf in EXCLUDE_SUFFIXES):
            continue

        # Remove 5-letter tickers ending in F/Y/Q (ADRs, foreign, bankruptcy)
        if len(t) == 5 and t[-1] in ("F", "Y", "Q"):
            continue

        # Remove SPAC-like tickers (4–5 letters ending in U/W/R)
        if len(t) >= 4 and t[-1] in ("U", "W", "R"):
            continue

        final.append(t)

    return sorted(set(final))

if __name__ == "__main__":
    raw = load_tickers()
    print(f"Loaded {len(raw)} tickers from v1.")

    clean = filter_v2(raw)
    print(f"Filtered down to {len(clean)} clean common stocks.")

    pd.Series(clean).to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")
