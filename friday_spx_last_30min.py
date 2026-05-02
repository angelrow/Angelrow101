"""
friday_spx_last_30min.py
========================
Analyses whether SPX tends to go up or down in the final 30 minutes of
trading on Fridays (3:30 PM – 4:00 PM ET).

Data sources (in priority order):
  1. Barchart 30-min CSV export  – main source (data from 2021 onward)
  2. yfinance ^GSPC 30-min       – fallback / supplement (last ~60 days only)

VIX data: pulled from yfinance ^VIX daily close for each Friday.
"""

import os
import sys
import warnings
import textwrap
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")          # headless rendering – no GUI required
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import yfinance as yf

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# 0.  Configuration
# ---------------------------------------------------------------------------

# Possible paths for the Barchart 30-min intraday CSV
BARCHART_CSV_CANDIDATES = [
    Path.home() / "Desktop" / "spx_intraday-30min_historical-data-05-02-2026.csv",
    Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs" / "Desktop"
        / "spx_intraday-30min_historical-data-05-02-2026.csv",
    # Also check inside the repo's data folder
    Path(__file__).parent / "data" / "spx_intraday-30min_historical-data-05-02-2026.csv",
]

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Trading session constants (all times in ET)
LAST_BAR_HOUR   = 15   # 3 PM
LAST_BAR_MINUTE = 30   # :30 → bar starts at 3:30 PM
MARKET_CLOSE    = (16, 0)   # 4:00 PM ET

# ── Matplotlib style ──────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "#0d1117",
    "axes.facecolor":   "#161b22",
    "axes.edgecolor":   "#30363d",
    "axes.labelcolor":  "#c9d1d9",
    "axes.titlecolor":  "#f0f6fc",
    "xtick.color":      "#8b949e",
    "ytick.color":      "#8b949e",
    "text.color":       "#c9d1d9",
    "grid.color":       "#21262d",
    "grid.linestyle":   "--",
    "grid.alpha":       0.5,
    "legend.facecolor": "#161b22",
    "legend.edgecolor": "#30363d",
})

UP_COLOR   = "#3fb950"   # green
DOWN_COLOR = "#f85149"   # red
NEUTRAL    = "#58a6ff"   # blue

# ---------------------------------------------------------------------------
# 1.  Load intraday SPX data
# ---------------------------------------------------------------------------

def _detect_barchart_timestamp(df: pd.DataFrame) -> pd.Series:
    """
    Barchart timestamps can appear as:
      - "04/25/2025 15:30"   (MM/DD/YYYY HH:MM)
      - "2025-04-25 15:30"   (ISO-ish)
      - Unix integer seconds
    Returns a UTC-aware or tz-naive pandas Series of datetimes (in ET).
    """
    col = df.index if df.index.name and "time" in df.index.name.lower() else df.iloc[:, 0]
    sample = str(col.iloc[0])

    if sample.isdigit():
        # Unix epoch
        ts = pd.to_datetime(col.astype(int), unit="s", utc=True)
        return ts.dt.tz_convert("America/New_York")

    # Try common Barchart formats
    for fmt in ("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M:%S",
                "%Y-%m-%d %H:%M:%S"):
        try:
            ts = pd.to_datetime(col, format=fmt)
            # Barchart exports are in CT (Chicago); some are in ET.
            # We treat as ET because SPX is quoted in ET business hours.
            return ts
        except Exception:
            continue

    # Fallback: let pandas guess
    ts = pd.to_datetime(col, infer_datetime_format=True)
    return ts


def load_barchart_csv(path: Path) -> pd.DataFrame | None:
    """
    Read a Barchart 30-min intraday CSV.
    Returns a DataFrame with columns: open, high, low, close, volume
    and a DatetimeIndex in ET (tz-naive, since Barchart usually exports ET).
    Returns None if the file can't be read.
    """
    if not path.exists():
        return None

    print(f"  Reading Barchart CSV: {path}")
    try:
        raw = pd.read_csv(path)
    except Exception as e:
        print(f"  ERROR reading CSV: {e}")
        return None

    print(f"  CSV columns: {list(raw.columns)}")
    print(f"  First row:   {raw.iloc[0].tolist()}")

    # Normalise column names to lowercase
    raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]

    # Find the timestamp column (first column or one named 'time' / 'date')
    time_col = None
    for name in ["time", "date", "datetime", "timestamp"]:
        if name in raw.columns:
            time_col = name
            break
    if time_col is None:
        time_col = raw.columns[0]   # assume first column

    # Parse timestamps — pass a single-column DataFrame; the function reads iloc[:,0]
    ts = _detect_barchart_timestamp(raw[[time_col]])

    raw.index = ts
    raw.index.name = "datetime"

    # Map Barchart column names → standard names
    # Barchart sometimes uses 'last' or 'latest' instead of 'close'
    rename_map = {}
    for src, dst in [("open", "open"), ("high", "high"), ("low", "low"),
                     ("close", "close"), ("last", "close"), ("latest", "close"),
                     ("volume", "volume"), ("vol", "volume")]:
        if src in raw.columns:
            rename_map[src] = dst

    df = raw.rename(columns=rename_map)[list(set(rename_map.values()))]

    # Remove duplicate 'close' if both 'close' and 'last' existed
    df = df.loc[:, ~df.columns.duplicated()]

    # Ensure numeric
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.sort_index(inplace=True)
    df.dropna(subset=["open", "close"], inplace=True)

    print(f"  Loaded {len(df):,} intraday bars  "
          f"({df.index[0].date()} → {df.index[-1].date()})")
    return df


def load_yfinance_intraday() -> pd.DataFrame | None:
    """
    Pull the last 60 days of 30-min SPX data from yfinance.
    Returns a DataFrame with columns: open, high, low, close, volume
    and a tz-aware DatetimeIndex in ET.
    """
    print("  Pulling ^GSPC 30-min from yfinance (last 60 days)…")
    try:
        raw = yf.download("^GSPC", period="60d", interval="30m",
                          auto_adjust=True, progress=False)
        if raw.empty:
            print("  yfinance returned no data.")
            return None
        # yfinance returns tz-aware index (America/New_York for US equities)
        raw.columns = [c[0].lower() if isinstance(c, tuple) else c.lower()
                       for c in raw.columns]
        raw.index.name = "datetime"
        raw.sort_index(inplace=True)
        print(f"  yfinance: {len(raw):,} bars  "
              f"({raw.index[0].date()} → {raw.index[-1].date()})")
        return raw
    except Exception as e:
        print(f"  yfinance error: {e}")
        return None


def load_spx_intraday() -> pd.DataFrame:
    """
    Load SPX 30-min intraday data.
    Priority: Barchart CSV → yfinance → crash with helpful message.
    """
    print("\n── Loading SPX 30-min intraday data ──────────────────────────────")

    barchart_df = None
    for candidate in BARCHART_CSV_CANDIDATES:
        barchart_df = load_barchart_csv(candidate)
        if barchart_df is not None:
            break

    yf_df = load_yfinance_intraday()

    if barchart_df is None and yf_df is None:
        sys.exit(
            "\nERROR: Could not load any SPX intraday data.\n"
            "Please place the Barchart CSV at one of these paths:\n"
            + "\n".join(f"  {p}" for p in BARCHART_CSV_CANDIDATES)
        )

    if barchart_df is not None and yf_df is not None:
        # Combine: use Barchart as primary, fill recent gaps with yfinance
        # Make both tz-naive for merging
        if hasattr(barchart_df.index, "tz") and barchart_df.index.tz is not None:
            barchart_df.index = barchart_df.index.tz_localize(None)
        if hasattr(yf_df.index, "tz") and yf_df.index.tz is not None:
            yf_df.index = yf_df.index.tz_localize(None)

        combined = pd.concat([barchart_df, yf_df])
        combined = combined[~combined.index.duplicated(keep="first")]
        combined.sort_index(inplace=True)
        print(f"  Combined total: {len(combined):,} bars")
        return combined

    df = barchart_df if barchart_df is not None else yf_df

    # Normalise to tz-naive for consistency
    if hasattr(df.index, "tz") and df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    return df


# ---------------------------------------------------------------------------
# 2.  Isolate Friday 3:30 PM bars
# ---------------------------------------------------------------------------

def extract_friday_bars(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a full intraday DataFrame, extract:
      - Only Friday bars
      - Specifically the bar that starts at 3:30 PM ET

    Also computes:
      - Day_Open:  first bar open price on that Friday
      - Day_Close: 3:30 PM bar close (= market close for the day)
      - Change_Points / Change_Pct:  move from 3:30 open → 3:30 close
      - Day_Direction: was the day UP or DOWN up to 3:30 PM?
    """
    print("\n── Extracting Friday 3:30 PM bars ───────────────────────────────")

    # Ensure datetime index is proper
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # Add helper columns
    df["_weekday"] = df.index.weekday   # Monday=0, Friday=4
    df["_date"]    = df.index.date
    df["_hour"]    = df.index.hour
    df["_minute"]  = df.index.minute

    # ── Step 1: Collect all Friday bars ──────────────────────────────────
    fridays_all = df[df["_weekday"] == 4].copy()

    # ── Step 2: For each Friday, get the daily open (first bar's Open) ──
    daily_open = (
        fridays_all.groupby("_date")["open"].first().rename("Day_Open")
    )

    # ── Step 3: Filter to 3:30 PM bars ───────────────────────────────────
    mask_330 = (fridays_all["_hour"] == LAST_BAR_HOUR) & \
               (fridays_all["_minute"] == LAST_BAR_MINUTE)
    bars_330 = fridays_all[mask_330].copy()

    if bars_330.empty:
        # Some Barchart exports use the bar *end* time, so 3:30 PM bar is
        # labelled 4:00 PM.  Try 4:00 PM bars on Fridays as fallback.
        print("  No 3:30 PM bars found — trying 4:00 PM label (bar-end convention)")
        mask_400 = (fridays_all["_hour"] == 16) & (fridays_all["_minute"] == 0)
        bars_330 = fridays_all[mask_400].copy()
        if bars_330.empty:
            # Last resort: take the last bar of each Friday
            print("  Still no match — using last bar of each Friday")
            bars_330 = fridays_all.groupby("_date").last().copy()
            bars_330.index = pd.to_datetime(bars_330.index)

    # De-dup: keep only one bar per Friday (in case of time zone edge cases)
    bars_330 = bars_330[~bars_330["_date"].duplicated(keep="first")]

    # ── Step 4: Attach daily open ─────────────────────────────────────────
    bars_330 = bars_330.join(daily_open, on="_date")

    # ── Step 5: Core metrics ──────────────────────────────────────────────
    bars_330["Bar_Open_330pm"]  = bars_330["open"]
    bars_330["Bar_Close_400pm"] = bars_330["close"]
    bars_330["Change_Points"]   = bars_330["close"] - bars_330["open"]
    bars_330["Change_Pct"]      = (bars_330["Change_Points"] / bars_330["open"]) * 100

    # Day direction: open of day → 3:30 PM open (price up to that point)
    bars_330["Day_Direction"] = np.where(
        bars_330["open"] >= bars_330["Day_Open"], "UP", "DOWN"
    )

    # ── Step 6: Filter out market half-days ───────────────────────────────
    # On early-close days (1 PM ET), there is no 3:30 PM bar —
    # so we simply wouldn't have found one.  This is already handled above.

    # ── Step 7: Build final output ────────────────────────────────────────
    result = bars_330[["_date", "Day_Open", "Bar_Open_330pm", "Bar_Close_400pm",
                        "Change_Points", "Change_Pct", "Day_Direction"]].copy()
    result.rename(columns={"_date": "Date"}, inplace=True)
    result["Date"] = pd.to_datetime(result["Date"])
    result.sort_values("Date", inplace=True)
    result.reset_index(drop=True, inplace=True)

    print(f"  Found {len(result):,} Friday 3:30 PM bars  "
          f"({result['Date'].min().date()} → {result['Date'].max().date()})")
    return result


# ---------------------------------------------------------------------------
# 3.  Attach VIX data
# ---------------------------------------------------------------------------

def attach_vix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Download daily VIX close from yfinance and join on Date.
    Adds a VIX_Close column and a VIX_Regime category.
    """
    print("\n── Downloading VIX daily data (yfinance) ───────────────────────")
    start = df["Date"].min() - timedelta(days=5)
    end   = df["Date"].max() + timedelta(days=1)

    try:
        vix_raw = yf.download("^VIX", start=start.strftime("%Y-%m-%d"),
                              end=end.strftime("%Y-%m-%d"),
                              interval="1d", auto_adjust=True, progress=False)
        if vix_raw.empty:
            raise ValueError("empty result")

        vix_raw.columns = [c[0].lower() if isinstance(c, tuple) else c.lower()
                           for c in vix_raw.columns]
        vix_close = vix_raw["close"].copy()
        vix_close.index = pd.to_datetime(vix_close.index).tz_localize(None)
        vix_close.index = vix_close.index.normalize()
        vix_close.name = "VIX_Close"

        # Force the index column to a known name regardless of yfinance version
        vix_frame = vix_close.reset_index()
        vix_frame.columns = ["Date_norm", "VIX_Close"]
        vix_frame["Date_norm"] = pd.to_datetime(vix_frame["Date_norm"]).dt.normalize()

        df = df.copy()
        df["Date_norm"] = pd.to_datetime(df["Date"]).dt.normalize()
        df = df.merge(vix_frame, on="Date_norm", how="left")
        df.drop(columns=["Date_norm"], errors="ignore", inplace=True)

        print(f"  VIX data attached — missing on {df['VIX_Close'].isna().sum()} Fridays")
    except Exception as e:
        print(f"  WARNING: Could not fetch VIX ({e}) — VIX columns will be NaN")
        df["VIX_Close"] = np.nan

    # ── VIX regime bins ──────────────────────────────────────────────────
    bins   = [0, 15, 25, 35, np.inf]
    labels = ["<15", "15–25", "25–35", ">35"]
    df["VIX_Regime"] = pd.cut(df["VIX_Close"], bins=bins, labels=labels)

    return df


# ---------------------------------------------------------------------------
# 4.  OPEX flag (third Friday of each month)
# ---------------------------------------------------------------------------

def add_opex_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mark the third Friday of each month as Is_OPEX = True.
    """
    def is_third_friday(dt):
        # Count Fridays in the month up to and including this date
        first = dt.replace(day=1)
        friday_count = 0
        current = first
        while current <= dt:
            if current.weekday() == 4:
                friday_count += 1
            current += timedelta(days=1)
        return friday_count == 3 and dt.weekday() == 4

    df = df.copy()
    df["Is_OPEX"] = df["Date"].apply(is_third_friday)
    return df


# ---------------------------------------------------------------------------
# 5.  Core statistics
# ---------------------------------------------------------------------------

def compute_stats(df: pd.DataFrame) -> dict:
    """
    Compute all required statistics on the Friday 3:30 PM bar dataset.
    Returns a dict of scalar stats and sub-DataFrames.
    """
    n_total   = len(df)
    n_down    = (df["Change_Points"] < 0).sum()
    n_up      = (df["Change_Points"] > 0).sum()
    n_flat    = n_total - n_down - n_up

    win_rate_short = (n_down / n_total) * 100   # % of Fridays that closed down

    down_moves = df.loc[df["Change_Points"] < 0, "Change_Points"]
    up_moves   = df.loc[df["Change_Points"] > 0, "Change_Points"]
    down_pct   = df.loc[df["Change_Points"] < 0, "Change_Pct"]
    up_pct     = df.loc[df["Change_Points"] > 0, "Change_Pct"]

    avg_move_all    = df["Change_Points"].mean()
    std_move_all    = df["Change_Points"].std()
    sharpe_like     = avg_move_all / std_move_all if std_move_all != 0 else np.nan

    # EV: shorting at 3:30, exit at close
    # Short P&L = -(Change_Points), i.e. positive when market falls
    short_pnl       = -df["Change_Points"]
    avg_short_pnl   = short_pnl.mean()

    # ── By VIX regime ────────────────────────────────────────────────────
    vix_stats = (
        df.groupby("VIX_Regime", observed=True)["Change_Points"]
          .agg(count="count", mean="mean", median="median", std="std")
          .round(2)
    )

    # ── By day direction ─────────────────────────────────────────────────
    day_dir_stats = (
        df.groupby("Day_Direction")["Change_Points"]
          .agg(count="count", mean="mean", median="median", std="std")
          .round(2)
    )

    # ── By month ──────────────────────────────────────────────────────────
    df["Month"] = df["Date"].dt.month
    month_stats = (
        df.groupby("Month")["Change_Pct"]
          .mean()
          .rename("avg_pct_change")
          .round(4)
    )

    # ── OPEX vs non-OPEX ─────────────────────────────────────────────────
    opex_stats = (
        df.groupby("Is_OPEX")["Change_Points"]
          .agg(count="count", mean="mean", median="median", std="std")
          .round(2)
    )

    return {
        "n_total":         n_total,
        "n_down":          n_down,
        "n_up":            n_up,
        "n_flat":          n_flat,
        "win_rate_short":  win_rate_short,
        "avg_down_pts":    down_moves.mean() if len(down_moves) else np.nan,
        "avg_up_pts":      up_moves.mean()   if len(up_moves)   else np.nan,
        "avg_down_pct":    down_pct.mean()   if len(down_pct)   else np.nan,
        "avg_up_pct":      up_pct.mean()     if len(up_pct)     else np.nan,
        "med_down_pts":    down_moves.median() if len(down_moves) else np.nan,
        "med_up_pts":      up_moves.median()   if len(up_moves)   else np.nan,
        "largest_drop":    down_moves.min()    if len(down_moves) else np.nan,
        "largest_rally":   up_moves.max()      if len(up_moves)   else np.nan,
        "avg_move_all":    avg_move_all,
        "std_move_all":    std_move_all,
        "sharpe_like":     sharpe_like,
        "avg_short_pnl":   avg_short_pnl,
        "vix_stats":       vix_stats,
        "day_dir_stats":   day_dir_stats,
        "month_stats":     month_stats,
        "opex_stats":      opex_stats,
    }


# ---------------------------------------------------------------------------
# 6.  Console output
# ---------------------------------------------------------------------------

def print_stats(s: dict, df: pd.DataFrame) -> None:
    SEP  = "=" * 62
    sep2 = "-" * 62
    MONTH_NAMES = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                   7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

    print(f"\n{SEP}")
    print("  SPX  ·  FRIDAY LAST-30-MIN ANALYSIS  (3:30–4:00 PM ET)")
    print(f"{SEP}")
    print(f"  Dataset covers:  {df['Date'].min().date()}  →  {df['Date'].max().date()}")
    print(f"{sep2}")

    print("\n  ── CORE COUNTS ─────────────────────────────────────────────")
    print(f"  Total Fridays analysed:   {s['n_total']}")
    print(f"  Closed DOWN (bearish):    {s['n_down']}  ({s['n_down']/s['n_total']*100:.1f}%)")
    print(f"  Closed UP   (bullish):    {s['n_up']}  ({s['n_up']/s['n_total']*100:.1f}%)")
    print(f"  Flat (no change):         {s['n_flat']}")

    print("\n  ── MOVE SIZE ───────────────────────────────────────────────")
    print(f"  Average DOWN move:     {s['avg_down_pts']:>+8.2f} pts  ({s['avg_down_pct']:>+.3f}%)")
    print(f"  Average UP   move:     {s['avg_up_pts']:>+8.2f} pts  ({s['avg_up_pct']:>+.3f}%)")
    print(f"  Median  DOWN move:     {s['med_down_pts']:>+8.2f} pts")
    print(f"  Median  UP   move:     {s['med_up_pts']:>+8.2f} pts")
    print(f"  Largest drop:          {s['largest_drop']:>+8.2f} pts")
    print(f"  Largest rally:         {s['largest_rally']:>+8.2f} pts")

    print("\n  ── SHORT EDGE (if you shorted at 3:30 every Friday) ────────")
    print(f"  Win rate:              {s['win_rate_short']:.1f}%")
    print(f"  Avg P&L per trade:     {s['avg_short_pnl']:>+8.2f} SPX pts")
    print(f"  Sharpe-like ratio:     {s['sharpe_like']:>+8.4f}  (mean/stdev of 30-min return)")

    print("\n  ── BY VIX REGIME ───────────────────────────────────────────")
    print(f"  {'Regime':<10}  {'N':>5}  {'Avg (pts)':>10}  {'Median':>8}  {'Std':>8}")
    print(f"  {'-'*10}  {'-'*5}  {'-'*10}  {'-'*8}  {'-'*8}")
    for regime, row in s["vix_stats"].iterrows():
        print(f"  {str(regime):<10}  {int(row['count']):>5}  "
              f"{row['mean']:>+10.2f}  {row['median']:>+8.2f}  {row['std']:>8.2f}")

    print("\n  ── BY DAY DIRECTION (open → 3:30 PM) ───────────────────────")
    print(f"  {'Direction':<10}  {'N':>5}  {'Avg (pts)':>10}  {'Median':>8}  {'Std':>8}")
    print(f"  {'-'*10}  {'-'*5}  {'-'*10}  {'-'*8}  {'-'*8}")
    for direction, row in s["day_dir_stats"].iterrows():
        print(f"  {direction:<10}  {int(row['count']):>5}  "
              f"{row['mean']:>+10.2f}  {row['median']:>+8.2f}  {row['std']:>8.2f}")

    print("\n  ── BY MONTH (avg last-30-min return, %) ────────────────────")
    months = s["month_stats"]
    line = ""
    for m, val in months.items():
        line += f"  {MONTH_NAMES[m]}: {val:>+.3f}%"
        if m % 4 == 0:
            print(line)
            line = ""
    if line.strip():
        print(line)

    print("\n  ── OPEX vs NON-OPEX ────────────────────────────────────────")
    opex_df = s["opex_stats"]
    for flag, row in opex_df.iterrows():
        label = "OPEX Friday    " if flag else "Non-OPEX Friday"
        print(f"  {label}  N={int(row['count'])}  "
              f"avg={row['mean']:>+.2f} pts  median={row['median']:>+.2f} pts")

    # ── KEY FINDING ──────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print("  KEY FINDING")
    print(f"{SEP}")
    wr = s["win_rate_short"]
    avg = s["avg_short_pnl"]
    sh  = s["sharpe_like"]

    if wr >= 55 and avg > 0:
        verdict = (f"YES — there appears to be a SHORT edge. {wr:.1f}% of Fridays "
                   f"close lower in the final 30 min, with an average gain of "
                   f"{avg:+.2f} SPX pts per trade (Sharpe-like: {sh:.3f}).")
    elif wr <= 45 and avg < 0:
        verdict = (f"REVERSED — the last 30 min on Fridays BUYS, not sells. "
                   f"Only {wr:.1f}% closed down. Avg short P&L = {avg:+.2f} pts.")
    else:
        verdict = (f"MIXED — no strong directional edge. Win rate for shorts: "
                   f"{wr:.1f}%, avg short P&L = {avg:+.2f} pts (Sharpe-like: {sh:.3f}).")

    print(textwrap.fill(f"  {verdict}", width=62, subsequent_indent="  "))
    print(SEP + "\n")


# ---------------------------------------------------------------------------
# 7.  Charts
# ---------------------------------------------------------------------------

def _save(fig: plt.Figure, filename: str) -> None:
    path = OUTPUT_DIR / filename
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_up_down_count(s: dict) -> None:
    """Bar chart: Up vs Down Friday count."""
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(["Down (bearish)", "Up (bullish)", "Flat"],
                  [s["n_down"], s["n_up"], s["n_flat"]],
                  color=[DOWN_COLOR, UP_COLOR, NEUTRAL], width=0.5)
    for bar in bars:
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.5,
                str(int(bar.get_height())), ha="center", va="bottom", fontsize=11)
    ax.set_title("Friday Last-30-Min: Up vs Down Count")
    ax.set_ylabel("# of Fridays")
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)
    _save(fig, "01_up_down_count.png")


def chart_return_histogram(df: pd.DataFrame) -> None:
    """Histogram of 30-min returns in points."""
    fig, ax = plt.subplots(figsize=(8, 4))
    n, bins, patches = ax.hist(df["Change_Points"], bins=30, edgecolor="#21262d")
    for patch, left in zip(patches, bins):
        patch.set_facecolor(DOWN_COLOR if left < 0 else UP_COLOR)
    ax.axvline(0, color="white", linewidth=1, linestyle="--", alpha=0.6)
    ax.axvline(df["Change_Points"].mean(), color="yellow", linewidth=1.5,
               linestyle="--", label=f"Mean {df['Change_Points'].mean():+.1f} pts")
    ax.set_title("Distribution of Friday Last-30-Min Returns (SPX Points)")
    ax.set_xlabel("SPX Points")
    ax.set_ylabel("Frequency")
    ax.legend()
    _save(fig, "02_return_histogram.png")


def chart_equity_curve(df: pd.DataFrame) -> None:
    """Cumulative P&L if shorting every Friday at 3:30 PM."""
    pnl_series = -df["Change_Points"].reset_index(drop=True)
    cum_pnl    = pnl_series.cumsum()

    fig, ax = plt.subplots(figsize=(10, 4))
    color = np.where(cum_pnl >= 0, UP_COLOR, DOWN_COLOR)
    ax.fill_between(cum_pnl.index, cum_pnl, 0,
                    where=(cum_pnl >= 0), color=UP_COLOR, alpha=0.3)
    ax.fill_between(cum_pnl.index, cum_pnl, 0,
                    where=(cum_pnl < 0),  color=DOWN_COLOR, alpha=0.3)
    ax.plot(cum_pnl.index, cum_pnl.values, color=NEUTRAL, linewidth=1.5)
    ax.axhline(0, color="white", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_title("Cumulative P&L — Short SPX Every Friday at 3:30 PM (exit at close)")
    ax.set_xlabel("Trade #")
    ax.set_ylabel("Cumulative SPX Points")

    # Label final value
    final = cum_pnl.iloc[-1]
    ax.annotate(f"{final:+.1f}", xy=(cum_pnl.index[-1], final),
                xytext=(-20, 10), textcoords="offset points",
                color="white", fontsize=9)
    _save(fig, "03_equity_curve.png")


def chart_vix_regime(s: dict) -> None:
    """Grouped bar: average 30-min return by VIX regime."""
    vix = s["vix_stats"].reset_index()
    if vix.empty or vix["count"].sum() == 0:
        print("  Skipping VIX regime chart (no VIX data)")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    colors = [DOWN_COLOR if v < 0 else UP_COLOR for v in vix["mean"]]
    bars = ax.bar(vix["VIX_Regime"].astype(str), vix["mean"],
                  color=colors, width=0.5)
    for bar, n in zip(bars, vix["count"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (0.3 if bar.get_height() >= 0 else -1.5),
                f"n={int(n)}", ha="center", va="bottom", fontsize=8, color="white")
    ax.axhline(0, color="white", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_title("Avg Last-30-Min Return by VIX Regime (SPX Points)")
    ax.set_xlabel("VIX Level")
    ax.set_ylabel("Avg Change (pts)")
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)
    _save(fig, "04_vix_regime.png")


def chart_day_direction(s: dict) -> None:
    """Grouped bar: avg 30-min return split by whether day was UP or DOWN."""
    dd = s["day_dir_stats"].reset_index()
    if dd.empty:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    colors = [UP_COLOR if d == "UP" else DOWN_COLOR for d in dd["Day_Direction"]]
    bars = ax.bar(dd["Day_Direction"], dd["mean"], color=colors, width=0.4)
    for bar, n in zip(bars, dd["count"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (0.2 if bar.get_height() >= 0 else -1.2),
                f"n={int(n)}", ha="center", va="bottom", fontsize=9, color="white")
    ax.axhline(0, color="white", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_title("Avg Last-30-Min Return: Day was UP vs DOWN")
    ax.set_xlabel("Day Direction (open → 3:30 PM)")
    ax.set_ylabel("Avg Change (pts)")
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)
    _save(fig, "05_day_direction.png")


def chart_by_month(s: dict) -> None:
    """Bar chart: average last-30-min % return by calendar month."""
    MONTH_ABBR = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                  7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    ms = s["month_stats"].reset_index()
    ms["label"] = ms["Month"].map(MONTH_ABBR)
    colors = [DOWN_COLOR if v < 0 else UP_COLOR for v in ms["avg_pct_change"]]

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(ms["label"], ms["avg_pct_change"], color=colors, width=0.6)
    ax.axhline(0, color="white", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_title("Avg Last-30-Min Return by Month (%)")
    ax.set_ylabel("Avg % Change")
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=2))
    _save(fig, "06_by_month.png")


def chart_opex(s: dict) -> None:
    """Bar chart: OPEX vs non-OPEX Friday average return."""
    opex = s["opex_stats"].reset_index()
    opex["label"] = opex["Is_OPEX"].map({True: "OPEX Friday", False: "Non-OPEX Friday"})
    colors = [DOWN_COLOR if v < 0 else UP_COLOR for v in opex["mean"]]

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(opex["label"], opex["mean"], color=colors, width=0.4)
    for bar, n in zip(bars, opex["count"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (0.2 if bar.get_height() >= 0 else -1.2),
                f"n={int(n)}", ha="center", va="bottom", fontsize=9, color="white")
    ax.axhline(0, color="white", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_title("OPEX vs Non-OPEX Friday — Avg Last-30-Min Return (pts)")
    ax.set_ylabel("Avg Change (pts)")
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)
    _save(fig, "07_opex_vs_nonopex.png")


def generate_all_charts(df: pd.DataFrame, s: dict) -> None:
    print("\n── Generating charts ─────────────────────────────────────────")
    chart_up_down_count(s)
    chart_return_histogram(df)
    chart_equity_curve(df)
    chart_vix_regime(s)
    chart_day_direction(s)
    chart_by_month(s)
    chart_opex(s)


# ---------------------------------------------------------------------------
# 8.  Export processed data to CSV
# ---------------------------------------------------------------------------

def export_csv(df: pd.DataFrame) -> None:
    out_cols = ["Date", "Day_Open", "Bar_Open_330pm", "Bar_Close_400pm",
                "Change_Points", "Change_Pct", "VIX_Close",
                "Day_Direction", "Is_OPEX"]
    # Only include columns that exist
    out_cols = [c for c in out_cols if c in df.columns]
    out_path = Path(__file__).parent / "friday_last_30min_data.csv"
    df[out_cols].to_csv(out_path, index=False, float_format="%.4f")
    print(f"\n  Exported processed data → {out_path}")


# ---------------------------------------------------------------------------
# 9.  Demo / synthetic data generator
# ---------------------------------------------------------------------------

def generate_demo_data() -> pd.DataFrame:
    """
    Build ~4 years of synthetic 30-min SPX bars (2021-01-04 → 2024-12-31)
    with realistic drift and volatility so every pipeline step can be tested
    without a real CSV file.  Run with:  python friday_spx_last_30min.py --demo
    """
    print("\n  [DEMO] Generating synthetic SPX 30-min data (2021-2024)…")
    rng = np.random.default_rng(42)

    # Build a date range of 30-min bars for US market hours (9:30–16:00 ET)
    dates = pd.date_range("2021-01-04", "2024-12-31", freq="B")
    bar_times = pd.timedelta_range("9:30:00", "15:30:00", freq="30min")  # 13 bars/day

    rows = []
    price = 3750.0
    for day in dates:
        daily_vol = rng.uniform(0.005, 0.025)
        for bt in bar_times:
            ts = day + bt
            ret  = rng.normal(0.00003, daily_vol / 13)
            open_  = price
            close_ = price * (1 + ret)
            high_  = max(open_, close_) * (1 + abs(rng.normal(0, 0.0005)))
            low_   = min(open_, close_) * (1 - abs(rng.normal(0, 0.0005)))
            rows.append({"datetime": ts, "open": open_,
                         "high": high_, "low": low_, "close": close_})
            price = close_

    df = pd.DataFrame(rows).set_index("datetime")
    df.index = pd.to_datetime(df.index)
    print(f"  [DEMO] {len(df):,} synthetic bars generated.")
    return df


# ---------------------------------------------------------------------------
# 10.  Main
# ---------------------------------------------------------------------------

def _run_pipeline(raw_df: pd.DataFrame) -> None:
    friday_df = extract_friday_bars(raw_df)

    if friday_df.empty:
        sys.exit("ERROR: No Friday 3:30 PM bars found in the data.")

    friday_df = attach_vix(friday_df)
    friday_df = add_opex_flag(friday_df)
    stats     = compute_stats(friday_df)

    print_stats(stats, friday_df)
    generate_all_charts(friday_df, stats)
    export_csv(friday_df)

    print("\nDone.  Charts saved to:", OUTPUT_DIR.resolve())


def main() -> None:
    print("\n" + "=" * 62)
    print("  SPX FRIDAY LAST-30-MIN ANALYSIS")
    print("=" * 62)

    demo_mode = "--demo" in sys.argv

    if demo_mode:
        print("  *** DEMO MODE — using synthetic data ***")
        raw_df = generate_demo_data()
    else:
        raw_df = load_spx_intraday()

    _run_pipeline(raw_df)


if __name__ == "__main__":
    main()
