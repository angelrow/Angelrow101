#!/usr/bin/env python3
"""Fetch latest SPY price and VIX value via yfinance.
Writes data/spy.json and data/vix.json.
Always exits 0 — partial failures are logged but don't spam Actions emails.
"""

import json
import os
import sys
from datetime import datetime, timezone

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed", file=sys.stderr)
    sys.exit(1)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')


def fetch_price(ticker_symbol: str) -> float | None:
    """Return the most recent price for the given ticker, or None on any failure."""
    try:
        ticker = yf.Ticker(ticker_symbol)

        # fast_info.last_price is near real-time during market hours
        price = getattr(ticker.fast_info, 'last_price', None)
        if price and float(price) > 0:
            return float(price)

        # Fall back to the most recent close from recent history
        hist = ticker.history(period='5d')
        if not hist.empty:
            return float(hist['Close'].iloc[-1])

    except Exception as exc:
        print(f"WARNING: Could not fetch {ticker_symbol}: {exc}", file=sys.stderr)

    return None


def write_json(filename: str, symbol: str, price: float | None) -> bool:
    """Write price JSON to data/<filename>. Returns True on success."""
    if price is None:
        print(f"WARNING: Skipping {filename} — no price available", file=sys.stderr)
        return False

    payload = {
        "symbol": symbol,
        "price": round(price, 2),
        "last_updated_utc": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "source": "yfinance",
    }

    path = os.path.join(DATA_DIR, filename)
    os.makedirs(DATA_DIR, exist_ok=True)

    with open(path, 'w') as fh:
        json.dump(payload, fh, indent=2)

    print(f"OK: wrote {path} — {symbol} = {price:.2f}")
    return True


def main() -> None:
    spy_price = fetch_price("SPY")
    vix_price = fetch_price("^VIX")
    spx_price = fetch_price("^GSPC")

    spy_ok = write_json("spy.json", "SPY", spy_price)
    vix_ok = write_json("vix.json", "VIX", vix_price)
    spx_ok = write_json("spx.json", "SPX", spx_price)

    if not spy_ok and not vix_ok and not spx_ok:
        print("WARNING: All fetches failed — no files updated", file=sys.stderr)

    # Always exit 0 so transient Yahoo hiccups don't trigger failure emails
    sys.exit(0)


if __name__ == "__main__":
    main()
