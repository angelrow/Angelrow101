#!/usr/bin/env python3
"""
Market Weather Grid — Data Fetcher
Angelrow Trading Systems

Fetches SPY, VIX, and VIX3M data via yfinance, computes direction,
magnitude, and volatility composite scores, and appends a data point
to data/grid-data.json (rolling 200-point window).

Run on a 15-minute schedule during US market hours via GitHub Actions.
"""

import json
import math
import os
import sys
from datetime import datetime, timezone, timedelta

import numpy as np
import yfinance as yf

# ── Configuration ──────────────────────────────────────────────────────────────

MAX_POINTS = 400            # Rolling window size (~15 trading days at 26 pts/day)
DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'grid-data.json')

# Direction normalisation divisor — tune for good spread across -1 to +1
# 0.06 was too small (clamped at 1.0 in any bull market)
# 0.50 gives full range: strong bull ~0.9, neutral ~0, strong bear ~-0.9
DIRECTION_DIVISOR = 0.50

# Volatility thresholds (for reference — frontend uses these too)
VIX_FLOOR = 12
VIX_CEIL = 40

# ── Helpers ────────────────────────────────────────────────────────────────────

def clamp(value, lo, hi):
    """Clamp a value between lo and hi."""
    return max(lo, min(hi, value))


def days_to_next_friday():
    """Return calendar days until the next Friday (min 1)."""
    today = datetime.now(timezone.utc).date()
    days_ahead = 4 - today.weekday()  # Friday = 4
    if days_ahead <= 0:
        days_ahead += 7
    return max(days_ahead, 1)


def load_existing_data():
    """Load existing grid data from JSON file."""
    try:
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return []


def save_data(data):
    """Save grid data to JSON file, trimmed to MAX_POINTS."""
    trimmed = data[-MAX_POINTS:]
    with open(DATA_FILE, 'w') as f:
        json.dump(trimmed, f, indent=2)
    print(f"Saved {len(trimmed)} data points to {DATA_FILE}")


# ── Data Fetching ──────────────────────────────────────────────────────────────

def fetch_market_data():
    """Fetch all required market data from Yahoo Finance."""

    print("Fetching SPY data...")
    spy = yf.Ticker("SPY")
    spy_hist = spy.history(period="1y")
    if spy_hist.empty:
        raise RuntimeError("Failed to fetch SPY data")

    spy_price = float(spy_hist['Close'].iloc[-1])
    sma50 = float(spy_hist['Close'].tail(50).mean())
    sma200 = float(spy_hist['Close'].tail(200).mean())

    print("Fetching VIX data...")
    vix_ticker = yf.Ticker("^VIX")
    vix_hist = vix_ticker.history(period="1y")
    if vix_hist.empty:
        raise RuntimeError("Failed to fetch VIX data")

    vix = float(vix_hist['Close'].iloc[-1])
    vix_9ma = float(vix_hist['Close'].tail(9).mean())
    vix_52w_low = float(vix_hist['Close'].min())
    vix_52w_high = float(vix_hist['Close'].max())

    print("Fetching VIX3M data...")
    vix3m = None
    try:
        vix3m_ticker = yf.Ticker("^VIX3M")
        vix3m_hist = vix3m_ticker.history(period="5d")
        if not vix3m_hist.empty:
            vix3m = float(vix3m_hist['Close'].iloc[-1])
    except Exception as e:
        print(f"VIX3M fetch failed (will use fallback): {e}")

    # Build rolling 30-day expected move baseline from VIX history
    vix_30d = vix_hist['Close'].tail(30).values
    spy_30d = spy_hist['Close'].tail(30).values

    return {
        'spy_price': spy_price,
        'sma50': sma50,
        'sma200': sma200,
        'vix': vix,
        'vix_9ma': vix_9ma,
        'vix_52w_low': vix_52w_low,
        'vix_52w_high': vix_52w_high,
        'vix3m': vix3m,
        'vix_30d': vix_30d,
        'spy_30d': spy_30d,
    }


# ── Score Computation ──────────────────────────────────────────────────────────

def compute_direction(spy_price, sma50, sma200):
    """
    Direction score: -1 (strong bear) to +1 (strong bull).
    Based on price vs 50/200 SMA and SMA alignment.
    """
    price_vs_50 = (spy_price - sma50) / sma50
    price_vs_200 = (spy_price - sma200) / sma200
    sma_alignment = 1.0 if sma50 > sma200 else -1.0

    raw = (price_vs_50 * 2.0) + (price_vs_200 * 1.0) + (sma_alignment * 0.3)
    return clamp(raw / DIRECTION_DIVISOR, -1.0, 1.0)


def compute_magnitude(vix, spy_price, vix_30d, spy_30d):
    """
    Magnitude score: -1 (compression) to +1 (expansion).
    Compares current expected move to 30-day rolling baseline.
    """
    dte = days_to_next_friday()
    expected_move_pct = (vix / 100.0) * math.sqrt(dte / 365.0)

    # Rolling baseline: average daily expected move over last 30 days
    baseline_moves = []
    for i in range(len(vix_30d)):
        daily_em = (float(vix_30d[i]) / 100.0) * math.sqrt(dte / 365.0)
        baseline_moves.append(daily_em)

    baseline = float(np.mean(baseline_moves)) if baseline_moves else expected_move_pct

    if baseline == 0:
        return 0.0

    magnitude = (expected_move_pct - baseline) / baseline
    return clamp(magnitude, -1.0, 1.0)


def compute_vol_score(vix, vix_52w_low, vix_52w_high, vix3m):
    """
    Volatility composite score: 0 (clean) to 1 (extreme danger).
    Weighted blend of VIX level, IV Rank, and term structure.
    """
    # VIX level score: VIX 12 = 0, VIX 40 = 1
    vix_score = clamp((vix - VIX_FLOOR) / (VIX_CEIL - VIX_FLOOR), 0.0, 1.0)

    # IV Rank: current VIX percentile within 52-week range
    vix_range = vix_52w_high - vix_52w_low
    if vix_range > 0:
        ivr = clamp((vix - vix_52w_low) / vix_range, 0.0, 1.0)
    else:
        ivr = 0.5

    # Term structure: VIX vs VIX3M
    # Backwardation (VIX > VIX3M) = fear = high score
    # Contango (VIX < VIX3M) = normal = low score
    if vix3m is not None:
        if vix > vix3m:
            term_score = 0.8  # backwardation
        elif abs(vix - vix3m) < 1.0:
            term_score = 0.5  # flat
        else:
            term_score = 0.2  # contango (normal)
    else:
        # Fallback: use VIX level as rough proxy
        term_score = 0.5 if vix > 20 else 0.2

    vol_score = (vix_score * 0.5) + (ivr * 0.3) + (term_score * 0.2)
    return round(vol_score, 4)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Market Weather Grid — Data Fetcher")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    try:
        data = fetch_market_data()
    except Exception as e:
        print(f"ERROR fetching market data: {e}")
        sys.exit(1)

    spy_price = data['spy_price']
    sma50 = data['sma50']
    sma200 = data['sma200']
    vix = data['vix']
    vix3m = data['vix3m']

    # Compute scores
    direction = compute_direction(spy_price, sma50, sma200)
    magnitude = compute_magnitude(vix, spy_price, data['vix_30d'], data['spy_30d'])
    vol_score = compute_vol_score(vix, data['vix_52w_low'], data['vix_52w_high'], vix3m)

    # IV Rank as percentage
    vix_range = data['vix_52w_high'] - data['vix_52w_low']
    ivr_pct = round((vix - data['vix_52w_low']) / vix_range * 100, 1) if vix_range > 0 else 50.0
    ivr_pct = clamp(ivr_pct, 0, 100)

    # Expected move
    dte = days_to_next_friday()
    em_pct = round((vix / 100.0) * math.sqrt(dte / 365.0) * 100, 2)

    # Build data point
    point = {
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'dir': round(direction, 4),
        'mag': round(magnitude, 4),
        'volScore': vol_score,
        'spy': round(spy_price, 2),
        'vix': round(vix, 2),
        'ivr': round(ivr_pct, 1),
        'em': em_pct,
        'sma50': round(sma50, 2),
        'sma200': round(sma200, 2),
    }

    if vix3m is not None:
        point['vix3m'] = round(vix3m, 2)

    # Print summary
    print(f"\nSPY: ${spy_price:.2f}  |  SMA50: ${sma50:.2f}  |  SMA200: ${sma200:.2f}")
    print(f"VIX: {vix:.2f}  |  VIX3M: {vix3m or 'N/A'}  |  IVR: {ivr_pct:.1f}%")
    print(f"Expected Move: ±{em_pct}% ({dte}d to Friday)")
    print(f"\nDirection: {direction:+.4f}  |  Magnitude: {magnitude:+.4f}  |  VolScore: {vol_score:.4f}")

    # Regime label
    if vol_score < 0.35:
        regime = "CLEAN"
    elif vol_score < 0.70:
        regime = "CAUTION"
    else:
        regime = "NO TRADE"
    print(f"Regime: {regime}")

    # Save
    existing = load_existing_data()
    existing.append(point)
    save_data(existing)

    print("\nDone.")


if __name__ == '__main__':
    main()
