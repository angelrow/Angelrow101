"""
0DTE Entry-Surface Backtest Engine  (vectorised)
SPX 20-wide bull put spread: short k×EM-remaining below spot, long 20 lower.

Unit convention throughout: DatetimeIndex.asi8 gives microseconds in pandas 3.x.
Timestamp.value gives nanoseconds.  We normalise to MICROSECONDS everywhere:
  _ts_to_us(ts) = ts.value // 1000
  1 minute       = US_PER_MIN = 60_000_000 μs
"""

import json
import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import brentq, least_squares
from scipy.stats import norm

# ── Repo / data roots ────────────────────────────────────────────────────────
REPO      = Path(__file__).resolve().parents[2]
DATA      = REPO / "trading_data"
TOOLS     = Path(__file__).resolve().parent
CALIB_CSV = TOOLS / "calibration" / "calib_dataset.csv"
VIXD_DIR  = TOOLS / "calibration" / "vixd"

# ── Parameterised constants ───────────────────────────────────────────────────
ENTRY_TIMES = [
    "09:30","10:00","10:30","11:00","11:30","12:00",
    "12:30","13:00","13:30","14:00","14:30","15:00","15:30",
]
K_VALUES     = np.array([round(0.5 + 0.1*i, 1) for i in range(26)])
N_K          = len(K_VALUES)

SPREAD_WIDTH     = 20
STOP_MULTIPLE    = 4
ENTRY_FEE        = 2.35
EXIT_FEE_STOP    = 2.35
EXIT_FEE_EXPIRY  = 0.0
CREDIT_FLOOR     = 0.60
SLIPPAGE         = 0.05
MULTIPLIER       = 100
RISK_FREE        = 0.045
DIV_YIELD        = 0.013
# Road A window — VIXD history available from Apr 2023
IS_START         = pd.Timestamp("2023-04-03", tz="America/New_York")
IN_SAMPLE_END    = pd.Timestamp("2025-05-31", tz="America/New_York")
HOLDOUT_START    = pd.Timestamp("2025-06-01", tz="America/New_York")
HOLDOUT_END      = pd.Timestamp("2026-05-29", tz="America/New_York")
STALE_ENTRY_WINDOW = 5   # minutes

# Road A fog bands — signed median bias ($ per option) from VIXD calibration report.
# Net spread credit bias = FOG_NET_SCALE × band_bias (short − long, conservative approx.)
_FOG_BAND_BIAS = {
    "09:30": 0.38,  "10:00": 0.38,
    "10:30": 1.70,  "11:00": 1.70,  "11:30": 1.70,
    "12:00": 1.70,  "12:30": 1.70,
    "13:00": 1.01,  "13:30": 1.01,  "14:00": 1.01,
    "14:30": 1.01,
    "15:00": 0.51,  "15:30": 0.51,
}
FOG_NET_SCALE = 0.45  # net spread bias ≈ 0.45 × single-leg band bias (stated approx.)

# ── Unit helpers ──────────────────────────────────────────────────────────────
US_PER_MIN = 60_000_000   # microseconds per minute

def _ts_to_us(ts: pd.Timestamp) -> int:
    """Convert a tz-aware pd.Timestamp to microseconds since epoch (consistent with .asi8)."""
    return ts.value // 1000


# ── Vectorised BSM helpers ────────────────────────────────────────────────────

def _bsm_put_arr(S, K, T, r, q, sigma):
    """
    Vectorised European put price. S may be scalar or array; K, T, sigma arrays.
    """
    T     = np.asarray(T,     dtype=float)
    sigma = np.asarray(sigma, dtype=float)
    K     = np.asarray(K,     dtype=float)
    sqrt_T = np.sqrt(np.maximum(T, 1e-12))
    sq     = sigma * sqrt_T
    safe_sq = np.maximum(sq, 1e-12)
    d1 = np.where(
        T > 0,
        (np.log(S / K) + (r - q + 0.5*sigma**2)*T) / safe_sq,
        0.0,
    )
    d2  = d1 - sq
    er  = np.exp(-r * T)
    eq  = np.exp(-q * T)
    px  = K * er * norm.cdf(-d2) - S * eq * norm.cdf(-d1)
    return np.where(T <= 0, np.maximum(K - S, 0.0), px)


def _bsm_put_scalar(S, K, T, r, q, sigma):
    if T <= 0:
        return max(K - S, 0.0)
    sq = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5*sigma**2)*T) / sq
    d2 = d1 - sq
    return K*math.exp(-r*T)*norm.cdf(-d2) - S*math.exp(-q*T)*norm.cdf(-d1)


def _iv_arr(sigma_base, m, params, use_abs_m=False):
    """Vectorised IV surface — 3-parameter model (no VIX-level term).
    model_form 'm2':    sigma_base × max(a0 + b·m + c·m²,  0.05)
    model_form 'abs_m': sigma_base × max(a0 + b·m + c·|m|, 0.05)
    VIX enters only through sigma_base = VIX/100.
    """
    a0, b, c = params
    tail = c * np.abs(m) if use_abs_m else c * m**2
    raw  = a0 + b*m + tail
    return sigma_base * np.maximum(raw, 0.05)


# ── Data loading ──────────────────────────────────────────────────────────────

def _parse_barchart_csv(path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path, on_bad_lines="warn", low_memory=False)
    raw = raw[raw["Time"].str.match(r"^\d{4}-\d{2}-\d{2}", na=False)].copy()
    raw["Time"] = raw["Time"].str.strip('"')
    raw["Time"] = pd.to_datetime(raw["Time"], format="%Y-%m-%d %H:%M")
    raw = raw.dropna(subset=["Time"])
    expected = {"Time", "Open", "High", "Low", "Latest"}
    missing = expected - set(raw.columns)
    if missing:
        raise RuntimeError(
            f"HARD ERROR: Unrecognised headers in {path.name}: "
            f"expected {expected}, missing {missing}"
        )
    raw = raw.rename(columns={"Latest": "Close"})
    for col in ("Open", "High", "Low", "Close"):
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=["Open", "High", "Low", "Close"])
    raw = raw.sort_values("Time").reset_index(drop=True)
    raw["Time"] = raw["Time"].dt.tz_localize(
        "America/New_York", ambiguous="infer", nonexistent="shift_forward"
    )
    raw = raw.set_index("Time")
    return raw[["Open", "High", "Low", "Close"]]


def load_all_bars(symbol: str) -> pd.DataFrame:
    with open(DATA / "manifest.json") as f:
        manifest = json.load(f)
    frames = []
    for rel in manifest[symbol]:
        p = DATA / rel
        if not p.exists():
            raise RuntimeError(f"HARD ERROR: Missing data file: {p}")
        frames.append(_parse_barchart_csv(p))
    df = pd.concat(frames)
    df = df[~df.index.duplicated(keep="first")].sort_index()
    sample = df.loc["2024-01-02"].between_time("09:30", "16:00")
    if len(sample) < 100:
        raise RuntimeError(
            f"HARD ERROR: {symbol} bars for 2024-01-02 span only {len(sample)} "
            "RTH bars — possible timezone mismatch."
        )
    return df


# ── Settlement time ───────────────────────────────────────────────────────────

def settlement_time(day_spx: pd.DataFrame) -> pd.Timestamp | None:
    rth = day_spx.between_time("09:30", "16:05")
    if len(rth) == 0:
        return None
    last_ts = rth.index[-1]
    tz = last_ts.tzinfo
    cutoff = pd.Timestamp(last_ts.year, last_ts.month, last_ts.day, 13, 5, tzinfo=tz)
    if last_ts <= cutoff:
        return last_ts
    return pd.Timestamp(last_ts.year, last_ts.month, last_ts.day, 16, 0, tzinfo=tz)


# ── Calibration ───────────────────────────────────────────────────────────────

def run_calibration(spx: pd.DataFrame, vix: pd.DataFrame) -> tuple:
    """
    3-parameter model: IV = (VIXD/100) × max(a0 + b·m + c·|m|, 0.05),  c ≥ 0.
    σ_base = VIXD/100 — most recent 1-min VIXD bar at or before each print.
    VIXD files must exist at VIXD_DIR/{expiry_date}.csv; hard error if any missing.
    Returns (params, model_form, report).  params = (a0, b, c).  model_form = "abs_m".
    """
    if not CALIB_CSV.exists():
        raise RuntimeError(f"HARD ERROR: Calibration file missing: {CALIB_CSV}")
    calib = pd.read_csv(CALIB_CSV)
    n_rows = len(calib)
    if abs(n_rows - 4685) > 50:
        raise RuntimeError(
            f"HARD ERROR: calib_dataset.csv has {n_rows} rows, expected 4685 ± 50."
        )

    calib["ts"] = pd.to_datetime(calib["timestamp_et"]).dt.tz_localize(
        "America/New_York", ambiguous="infer", nonexistent="shift_forward"
    )
    calib["expiry_date"] = pd.to_datetime(calib["expiry"]).dt.date

    for exp in calib["expiry_date"].unique():
        try:
            bars = spx.loc[str(exp)]
        except KeyError:
            bars = pd.DataFrame()
        if len(bars) == 0:
            raise RuntimeError(
                f"HARD ERROR: Calibration expiry {exp} has no SPX bars in trading_data/."
            )

    spx_us = spx.index.asi8
    spx_cl = spx["Close"].values
    vix_us = vix.index.asi8
    vix_cl = vix["Close"].values
    ts_us  = pd.DatetimeIndex(calib["ts"]).asi8

    spx_idx = np.searchsorted(spx_us, ts_us, side="right") - 1
    vix_idx = np.searchsorted(vix_us, ts_us, side="right") - 1
    valid   = (spx_idx >= 0) & (vix_idx >= 0)
    calib   = calib[valid].copy().reset_index(drop=True)
    spx_idx = spx_idx[valid]; vix_idx = vix_idx[valid]; ts_us = ts_us[valid]

    S_arr   = spx_cl[spx_idx]
    K_arr   = calib["strike"].values.astype(float)
    px_arr  = calib["price_last"].values.astype(float)
    vol_arr = pd.to_numeric(calib["volume"], errors="coerce").fillna(0).values

    # Load VIXD files (one per calibration day) and compute per-print σ_base
    if not VIXD_DIR.exists():
        raise RuntimeError(f"HARD ERROR: VIXD directory missing: {VIXD_DIR}")
    sigma_arr = np.zeros(len(calib), dtype=float)
    for exp in sorted(calib["expiry_date"].unique()):
        path = VIXD_DIR / f"{exp}.csv"
        if not path.exists():
            raise RuntimeError(
                f"HARD ERROR: VIXD file missing for calibration day {exp}: {path}"
            )
        vixd_df  = _parse_barchart_csv(path)
        vixd_us  = vixd_df.index.asi8
        vixd_cl  = vixd_df["Close"].values
        day_mask = (calib["expiry_date"].astype(str) == str(exp)).values
        day_ts   = ts_us[day_mask]
        vi = np.searchsorted(vixd_us, day_ts, side="right") - 1
        n_before = int((vi < 0).sum())
        if n_before > 0:
            # Prints before the first VIXD bar (typically 09:30 vs 09:31 open);
            # clamp to first bar rather than hard-erroring — affects at most 1 min.
            print(f"      VIXD {exp}: {n_before} print(s) before first VIXD bar "
                  f"→ clamped to first bar ({pd.Timestamp(vixd_us[0], unit='us', tz='UTC').tz_convert('America/New_York').strftime('%H:%M')})")
            vi = np.maximum(vi, 0)
        sigma_arr[day_mask] = vixd_cl[vi] / 100.0
        print(f"      VIXD {exp}: {len(vixd_df)} bars  "
              f"range {vixd_cl.min():.2f}–{vixd_cl.max():.2f}  "
              f"({day_mask.sum()} prints mapped)")

    expiry_us = np.array([
        _ts_to_us(
            pd.Timestamp(str(row["expiry_date"])).tz_localize("America/New_York")
            .replace(hour=16, minute=0, second=0)
        )
        for _, row in calib.iterrows()
    ], dtype=np.int64)

    mins_rem  = (expiry_us - ts_us) / US_PER_MIN
    T_rem_arr = np.maximum(mins_rem, 0.0) / (252 * 390)

    valid2    = mins_rem > 0
    S_arr     = S_arr[valid2];    sigma_arr = sigma_arr[valid2]
    K_arr     = K_arr[valid2];    px_arr    = px_arr[valid2]
    vol_arr   = vol_arr[valid2];  T_rem_arr = T_rem_arr[valid2]
    mins_rem  = mins_rem[valid2]; calib     = calib[valid2].reset_index(drop=True)
    ts_us     = ts_us[valid2]

    denom = sigma_arr * np.sqrt(np.maximum(T_rem_arr, 1e-12))
    m_arr = np.log(K_arr / S_arr) / np.maximum(denom, 1e-12)

    # ── Filter: extrinsic ≥ 0.15, |m| ≤ 3, T_rem ≥ 30 min ───────────────────
    right_vals = calib["right"].str.upper().values if "right" in calib.columns \
                 else np.full(len(calib), "P")
    intrinsic  = np.where(right_vals == "P",
                          np.maximum(0.0, K_arr - S_arr),
                          np.maximum(0.0, S_arr - K_arr))
    extrinsic  = px_arr - intrinsic
    filt = (extrinsic >= 0.15) & (np.abs(m_arr) <= 3.0) & (mins_rem >= 30.0)

    print("      Filter: extrinsic≥$0.15, |m|≤3, T_rem≥30min")
    for exp in sorted(calib["expiry_date"].unique()):
        exp_mask = calib["expiry_date"].values == exp
        n_tot  = int(exp_mask.sum())
        n_surv = int((exp_mask & filt).sum())
        tag = "  *** HARD WARNING: <50 prints ***" if n_surv < 50 else ""
        print(f"        expiry {exp}: {n_surv}/{n_tot} survive{tag}")
    abs_m_pre = np.abs(m_arr)
    for lo, hi in [(0, 1), (1, 2), (2, 3)]:
        n_b = int(((abs_m_pre >= lo) & (abs_m_pre < hi) & filt).sum())
        print(f"        |m| [{lo},{hi}): {n_b} prints survive")
    print(f"        Total: {int(filt.sum())}/{len(calib)} prints survive filter")

    calib     = calib[filt].copy().reset_index(drop=True)
    S_arr     = S_arr[filt];    sigma_arr = sigma_arr[filt]
    K_arr     = K_arr[filt];    px_arr    = px_arr[filt]
    vol_arr   = vol_arr[filt];  T_rem_arr = T_rem_arr[filt]
    m_arr     = m_arr[filt];    mins_rem  = mins_rem[filt]

    if len(px_arr) < 10:
        raise RuntimeError("HARD ERROR: Fewer than 10 calibration prints survive filter.")

    # ── Weights: sqrt(min(vol,20)) ────────────────────────────────────────────
    w_arr = np.sqrt(np.minimum(vol_arr, 20.0))

    # ── Step 1: brentq IV inversion ──────────────────────────────────────────
    iv_market = np.full(len(px_arr), np.nan)
    for i in range(len(px_arr)):
        S, K, T, px = S_arr[i], K_arr[i], T_rem_arr[i], px_arr[i]
        try:
            lo_px = _bsm_put_scalar(S, K, T, RISK_FREE, DIV_YIELD, 0.001)
            hi_px = _bsm_put_scalar(S, K, T, RISK_FREE, DIV_YIELD, 20.0)
            if hi_px < px:
                iv_market[i] = 20.0
            elif lo_px > px:
                iv_market[i] = 0.001
            else:
                iv_market[i] = brentq(
                    lambda sig: _bsm_put_scalar(S, K, T, RISK_FREE, DIV_YIELD, sig) - px,
                    0.001, 20.0, xtol=1e-6, maxiter=50,
                )
        except Exception:
            pass

    # ── Fit: IV = σ_base × max(a0 + b·m + c·|m|, 0.05),  c ≥ 0 ─────────────
    # Bounds: a0 ∈ [-5,5], b ∈ [-10,10], c ∈ [0,5]
    BOUNDS_LO = np.array([-5., -10., 0.])
    BOUNDS_HI = np.array([ 5.,  10., 5.])

    abs_m_f = np.abs(m_arr)

    valid_iv = np.isfinite(iv_market) & (sigma_arr > 0)
    if valid_iv.sum() >= 3:
        Y  = iv_market[valid_iv] / sigma_arr[valid_iv]
        Xm = m_arr[valid_iv]
        Wv = w_arr[valid_iv]
        Xa = np.column_stack([np.ones(valid_iv.sum()), Xm, np.abs(Xm)])
        x0, _, _, _ = np.linalg.lstsq(Xa * Wv[:,None], Y * Wv, rcond=None)
        x0 = np.clip(x0, BOUNDS_LO, BOUNDS_HI)
    else:
        x0 = np.array([1.0, -0.3, 0.1])

    def residuals_abs(p):
        iv = sigma_arr * np.maximum(p[0] + p[1]*m_arr + p[2]*np.abs(m_arr), 0.05)
        return w_arr * (_bsm_put_arr(S_arr, K_arr, T_rem_arr, RISK_FREE, DIV_YIELD, iv) - px_arr)

    res    = least_squares(residuals_abs, x0, method="trf",
                           bounds=(BOUNDS_LO.tolist(), BOUNDS_HI.tolist()),
                           max_nfev=20000)
    params     = tuple(res.x)
    model_form = "abs_m"
    use_abs_m  = True

    # ── Report arrays ─────────────────────────────────────────────────────────
    iv_fit  = _iv_arr(sigma_arr, m_arr, params, use_abs_m=use_abs_m)
    px_fit  = _bsm_put_arr(S_arr, K_arr, T_rem_arr, RISK_FREE, DIV_YIELD, iv_fit)
    abs_err = np.abs(px_fit - px_arr)
    signed  = px_fit - px_arr          # model − market
    pct_err = np.where(px_arr > 0, abs_err / px_arr, np.nan)

    # Pass-gate subset: market price ≥ $0.50
    gate_mask = px_arr >= 0.50
    gate_med_pct = float(np.nanmedian(pct_err[gate_mask])) if gate_mask.sum() > 0 else np.nan

    # Per-expiry: dollar MAE, median |%err| (full), median |%err| (≥$0.50), signed median
    per_expiry: dict = {}
    for exp in sorted(calib["expiry_date"].unique()):
        mask = calib["expiry_date"].values == exp
        gm   = mask & gate_mask
        biased = abs(float(np.nanmedian(signed[mask]))) > 1.0   # flag if >$1 bias
        per_expiry[str(exp)] = {
            "n":                    int(mask.sum()),
            "n_gate":               int(gm.sum()),
            "mae":                  float(np.mean(abs_err[mask])),
            "median_abs_pct_err":   float(np.nanmedian(pct_err[mask])),
            "median_abs_pct_err_gate": float(np.nanmedian(pct_err[gm])) if gm.sum() > 0 else None,
            "signed_median_err":    float(np.nanmedian(signed[mask])),
            "bias_flag":            biased,
        }

    # Per |m| band
    per_band: dict = {}
    for lo, hi in [(0, 0.5), (0.5, 1), (1, 2), (2, 3)]:
        mask = (abs_m_f >= lo) & (abs_m_f < hi)
        gm   = mask & gate_mask
        key  = f"|m| {lo:.1f}-{hi:.1f}"
        per_band[key] = {
            "n":                    int(mask.sum()),
            "mae":                  float(np.mean(abs_err[mask])) if mask.any() else None,
            "median_abs_pct_err":   float(np.nanmedian(pct_err[mask])) if mask.any() else None,
            "median_abs_pct_err_gate": float(np.nanmedian(pct_err[gm])) if gm.sum() > 0 else None,
            "signed_median_err":    float(np.nanmedian(signed[mask])) if mask.any() else None,
        }

    # Per intraday time band
    ts_hm = calib["ts"].dt.hour * 60 + calib["ts"].dt.minute
    time_bands = [
        ("09:30–10:30",  9*60+30, 10*60+30),
        ("10:30–13:00", 10*60+30, 13*60),
        ("13:00–15:00", 13*60,    15*60),
        ("15:00–16:00", 15*60,    16*60),
    ]
    per_time_band: dict = {}
    for label, lo_m, hi_m in time_bands:
        mask = ((ts_hm >= lo_m) & (ts_hm < hi_m)).values
        gm   = mask & gate_mask
        per_time_band[label] = {
            "n":                    int(mask.sum()),
            "n_gate":               int(gm.sum()),
            "mae":                  float(np.mean(abs_err[mask])) if mask.any() else None,
            "median_abs_pct_err":   float(np.nanmedian(pct_err[mask])) if mask.any() else None,
            "median_abs_pct_err_gate": float(np.nanmedian(pct_err[gm])) if gm.sum() > 0 else None,
            "signed_median_err":    float(np.nanmedian(signed[mask])) if mask.any() else None,
        }

    overall_mae     = float(np.mean(abs_err))
    overall_med_pct = float(np.nanmedian(pct_err))
    biased_days     = [exp for exp, d in per_expiry.items() if d["bias_flag"]]

    report = {
        "params": {"a0": params[0], "b": params[1], "c": params[2]},
        "model_form": model_form,
        "n_calibration_prints": int(len(px_arr)),
        "n_gate_prints": int(gate_mask.sum()),
        "overall_mae": overall_mae,
        "overall_median_abs_pct_err": overall_med_pct,
        "gate_median_abs_pct_err": gate_med_pct,
        "per_expiry": per_expiry,
        "per_m_band": per_band,
        "per_time_band": per_time_band,
        "biased_days": biased_days,
        "warning_unreliable": gate_med_pct > 0.35,
    }
    return params, model_form, report


# ── Vectorised day simulation ─────────────────────────────────────────────────

def simulate_day(
    date: pd.Timestamp,
    day_spx: pd.DataFrame,
    vix_ts_us: np.ndarray,    # VIX index in μs — for vix_band classification only
    vix_close: np.ndarray,
    vixd_ts_us: np.ndarray,   # VIXD index in μs — for σ_base = VIXD/100
    vixd_close: np.ndarray,
    params: tuple,
    skipped_rows: list,
    use_abs_m: bool = False,
) -> list:
    """
    Simulate all (T, k) cells for one trading day.
    σ_base = VIXD/100 at each bar. VIX retained only for vix_band split.
    LOOK-AHEAD GUARD: all vol lookups use at-or-before the SPX bar timestamp.
    """
    settle_ts = settlement_time(day_spx)
    if settle_ts is None:
        return []

    settle_us  = _ts_to_us(settle_ts)
    tz         = settle_ts.tzinfo
    spx_us     = day_spx.index.asi8
    spx_close  = day_spx["Close"].values
    spx_low    = day_spx["Low"].values
    results: list = []

    for t_str in ENTRY_TIMES:
        h, m = int(t_str[:2]), int(t_str[3:])
        entry_us = _ts_to_us(
            pd.Timestamp(date.year, date.month, date.day, h, m, tzinfo=tz)
        )

        if entry_us >= settle_us:
            continue

        idx_e = int(np.searchsorted(spx_us, entry_us, side="right")) - 1
        if idx_e < 0:
            continue
        actual_us  = int(spx_us[idx_e])
        stale_mins = (entry_us - actual_us) / US_PER_MIN
        if stale_mins > STALE_ENTRY_WINDOW:
            continue
        is_stale = stale_mins > 0.5

        S = float(spx_close[idx_e])

        # VIX at entry — for vix_band only (LOOK-AHEAD GUARD: at-or-before actual_us)
        vi_vix_e = int(np.searchsorted(vix_ts_us, actual_us, side="right")) - 1
        if vi_vix_e < 0:
            continue
        vix_val = float(vix_close[vi_vix_e])
        vix_band = "<20" if vix_val < 20 else ("20-30" if vix_val <= 30 else ">30")

        # VIXD at entry — σ_base = VIXD/100 (LOOK-AHEAD GUARD: at-or-before actual_us)
        vi_vixd_e = int(np.searchsorted(vixd_ts_us, actual_us, side="right")) - 1
        if vi_vixd_e < 0:
            vi_vixd_e = 0   # clamp: VIXD may start at 09:31
        sigma_b = float(vixd_close[vi_vixd_e]) / 100.0

        mins_rem = (settle_us - actual_us) / US_PER_MIN
        if mins_rem <= 0:
            continue
        T_rem  = mins_rem / (252 * 390)
        EM_rem = S * sigma_b * math.sqrt(T_rem)

        # Strike arrays (all k at once)
        K_short = np.round((S - K_VALUES * EM_rem) / 5) * 5
        K_long  = K_short - SPREAD_WIDTH

        # Entry model mid (vectorised)
        denom_e = sigma_b * math.sqrt(T_rem) if T_rem > 0 else 1e-9
        m_s_e   = np.log(K_short / S) / denom_e
        m_l_e   = np.log(K_long  / S) / denom_e
        iv_s_e  = _iv_arr(sigma_b, m_s_e, params, use_abs_m)
        iv_l_e  = _iv_arr(sigma_b, m_l_e, params, use_abs_m)
        p_s_e   = _bsm_put_arr(S, K_short, T_rem, RISK_FREE, DIV_YIELD, iv_s_e)
        p_l_e   = _bsm_put_arr(S, K_long,  T_rem, RISK_FREE, DIV_YIELD, iv_l_e)
        credit  = (p_s_e - p_l_e) - SLIPPAGE     # (N_K,)

        filled_mask = credit >= CREDIT_FLOOR

        # Record skipped cells
        for j in range(N_K):
            if not filled_mask[j]:
                skipped_rows.append({
                    "date": str(date.date()), "entry_time": t_str,
                    "k": float(K_VALUES[j]),
                    "K_short": float(K_short[j]), "K_long": float(K_long[j]),
                    "credit": round(float(credit[j]), 4), "vix": round(vix_val, 2),
                })

        if not np.any(filled_mask):
            for j in range(N_K):
                results.append({
                    "date": date.date(), "entry_time": t_str, "k": float(K_VALUES[j]),
                    "status": "skipped", "credit": float(credit[j]),
                    "vix_entry": vix_val, "stale_entry": is_stale,
                    "vix_band": vix_band,
                })
            continue

        max_risk = (SPREAD_WIDTH - credit) * MULTIPLIER

        # Post-entry SPX bars
        post_mask = (spx_us > actual_us) & (spx_us <= settle_us)
        post_us   = spx_us[post_mask]           # μs
        post_cl   = spx_close[post_mask]
        post_lo   = spx_low[post_mask]
        n_bars    = len(post_us)

        # Settlement price: close of last bar at or before settle_us
        s_idx     = int(np.searchsorted(spx_us, settle_us, side="right")) - 1
        S_settle  = float(spx_close[s_idx]) if s_idx >= 0 else S

        if n_bars == 0:
            payoff  = (credit
                       - np.maximum(K_short - S_settle, 0)
                       + np.maximum(K_long  - S_settle, 0))
            net_pnl = np.where(filled_mask,
                               payoff * MULTIPLIER - ENTRY_FEE, np.nan)
            ret_pct = np.where(filled_mask, net_pnl / max_risk * 100.0, np.nan)
            for j in range(N_K):
                if not filled_mask[j]:
                    results.append({
                        "date": date.date(), "entry_time": t_str, "k": float(K_VALUES[j]),
                        "status": "skipped", "credit": float(credit[j]),
                        "vix_entry": vix_val, "stale_entry": is_stale,
                        "era": era_val, "vix_band": vix_band,
                    })
                else:
                    results.append({
                        "date": date.date(), "entry_time": t_str, "k": float(K_VALUES[j]),
                        "status": "filled", "credit": round(float(credit[j]),4),
                        "K_short": float(K_short[j]), "K_long": float(K_long[j]),
                        "S_entry": round(S,2), "vix_entry": round(vix_val,2),
                        "max_risk": round(float(max_risk[j]),2),
                        "net_pnl": round(float(net_pnl[j]),2),
                        "ret_pct": round(float(ret_pct[j]),4),
                        "stop_fired": False, "stop_low_flag": False,
                        "touch_flag": False, "stale_entry": is_stale,
                        "era": era_val, "vix_band": vix_band,
                    })
            continue

        # VIXD for each post-entry bar — σ_base = VIXD/100
        # LOOK-AHEAD GUARD LINE: vixd lookup uses post_us (SPX bar timestamps), never later
        vi_vix  = np.searchsorted(vix_ts_us,  post_us, side="right") - 1
        vi_vixd = np.searchsorted(vixd_ts_us, post_us, side="right") - 1
        vi_vix  = np.maximum(vi_vix,  0)
        vi_vixd = np.maximum(vi_vixd, 0)   # clamp for 09:31 VIXD start
        pv_vixd = vixd_close[vi_vixd]      # (n_bars,)
        psig    = pv_vixd / 100.0
        pT      = np.maximum((settle_us - post_us) / US_PER_MIN, 0.0) / (252*390)

        # Broadcast: (n_bars,1) × (1,N_K) → (n_bars,N_K)
        S_cl   = post_cl[:,None]
        S_lo   = post_lo[:,None]
        sig_t  = psig[:,None]
        T_t    = pT[:,None]
        sqT    = np.sqrt(np.maximum(T_t, 1e-12))
        denom_t = sig_t * sqT
        Ks     = K_short[None,:]
        Kl     = K_long[None,:]

        m_s = np.log(Ks / S_cl) / np.maximum(denom_t, 1e-12)
        m_l = np.log(Kl / S_cl) / np.maximum(denom_t, 1e-12)
        iv_s = _iv_arr(sig_t, m_s, params, use_abs_m)
        iv_l = _iv_arr(sig_t, m_l, params, use_abs_m)

        sq_s   = iv_s * sqT;   sq_l   = iv_l * sqT
        er_t   = np.exp(-RISK_FREE * T_t)
        eq_t   = np.exp(-DIV_YIELD * T_t)
        drift  = RISK_FREE - DIV_YIELD

        d1_s = np.where(T_t>0, (np.log(S_cl/Ks)+(drift+0.5*iv_s**2)*T_t)/np.maximum(sq_s,1e-12), 0.)
        d2_s = d1_s - sq_s
        d1_l = np.where(T_t>0, (np.log(S_cl/Kl)+(drift+0.5*iv_l**2)*T_t)/np.maximum(sq_l,1e-12), 0.)
        d2_l = d1_l - sq_l

        ps = np.where(T_t>0, Ks*er_t*norm.cdf(-d2_s)-S_cl*eq_t*norm.cdf(-d1_s), np.maximum(Ks-S_cl,0))
        pl = np.where(T_t>0, Kl*er_t*norm.cdf(-d2_l)-S_cl*eq_t*norm.cdf(-d1_l), np.maximum(Kl-S_cl,0))
        buyback_cl = (ps - pl) + SLIPPAGE

        # Low-price sensitivity (LOOK-AHEAD GUARD: same vix/T, different spot)
        m_sl = np.log(Ks / S_lo) / np.maximum(denom_t, 1e-12)
        m_ll = np.log(Kl / S_lo) / np.maximum(denom_t, 1e-12)
        iv_sl = _iv_arr(sig_t, m_sl, params, use_abs_m)
        iv_ll = _iv_arr(sig_t, m_ll, params, use_abs_m)
        sq_sl = iv_sl*sqT; sq_ll = iv_ll*sqT
        d1_sl = np.where(T_t>0,(np.log(S_lo/Ks)+(drift+0.5*iv_sl**2)*T_t)/np.maximum(sq_sl,1e-12),0.)
        d2_sl = d1_sl - sq_sl
        d1_ll = np.where(T_t>0,(np.log(S_lo/Kl)+(drift+0.5*iv_ll**2)*T_t)/np.maximum(sq_ll,1e-12),0.)
        d2_ll = d1_ll - sq_ll
        ps_lo = np.where(T_t>0, Ks*er_t*norm.cdf(-d2_sl)-S_lo*eq_t*norm.cdf(-d1_sl), np.maximum(Ks-S_lo,0))
        pl_lo = np.where(T_t>0, Kl*er_t*norm.cdf(-d2_ll)-S_lo*eq_t*norm.cdf(-d1_ll), np.maximum(Kl-S_lo,0))
        buyback_lo = (ps_lo - pl_lo) + SLIPPAGE

        touch_arr   = (post_lo[:,None] <= Ks).any(axis=0)
        thresh      = STOP_MULTIPLE * credit[None,:]
        stop_hit    = buyback_cl >= thresh
        stop_hit_lo = buyback_lo >= thresh
        has_stop    = stop_hit.any(axis=0)
        has_stop_lo = stop_hit_lo.any(axis=0)

        first_stop  = np.where(has_stop, np.argmax(stop_hit, axis=0), 0)
        k_idx       = np.arange(N_K)
        stop_price  = np.where(has_stop, buyback_cl[first_stop, k_idx], 0.0)

        payoff_exp  = (credit
                       - np.maximum(K_short - S_settle, 0)
                       + np.maximum(K_long  - S_settle, 0))
        net_stop  = (credit - stop_price)*MULTIPLIER - ENTRY_FEE - EXIT_FEE_STOP
        net_exp   = payoff_exp*MULTIPLIER - ENTRY_FEE
        net_pnl   = np.where(has_stop, net_stop, net_exp)
        ret_pct   = net_pnl / max_risk * 100.0

        for j in range(N_K):
            if not filled_mask[j]:
                results.append({
                    "date": date.date(), "entry_time": t_str, "k": float(K_VALUES[j]),
                    "status": "skipped", "credit": float(credit[j]),
                    "vix_entry": vix_val, "stale_entry": is_stale,
                    "vix_band": vix_band,
                })
            else:
                results.append({
                    "date": date.date(), "entry_time": t_str, "k": float(K_VALUES[j]),
                    "status": "filled",
                    "credit": round(float(credit[j]),4),
                    "K_short": float(K_short[j]), "K_long": float(K_long[j]),
                    "S_entry": round(S,2), "vix_entry": round(vix_val,2),
                    "max_risk": round(float(max_risk[j]),2),
                    "net_pnl": round(float(net_pnl[j]),2),
                    "ret_pct": round(float(ret_pct[j]),4),
                    "stop_fired": bool(has_stop[j]),
                    "stop_low_flag": bool(has_stop_lo[j]),
                    "touch_flag": bool(touch_arr[j]),
                    "stale_entry": is_stale,
                    "era": era_val, "vix_band": vix_band,
                })

    return results


# ── Statistics ────────────────────────────────────────────────────────────────

def cell_stats(trades_df: pd.DataFrame, n_attempted: int) -> dict:
    filled  = trades_df[trades_df["status"] == "filled"]
    skipped = trades_df[trades_df["status"] == "skipped"]
    n_f, n_s = len(filled), len(skipped)
    base = {
        "n_attempted": n_attempted,
        "n_filled": n_f,
        "n_skipped_floor": n_s,
        "median_credit_filled": round(float(filled["credit"].median()),4) if n_f>0 else None,
        "median_credit_skip":   round(float(skipped["credit"].median()),4) if n_s>0 else None,
        "skip_share": round(n_s/n_attempted,4) if n_attempted>0 else None,
    }
    if n_f == 0:
        base.update({
            "expectancy_pct": None, "expectancy_se": None, "expectancy_tstat": None,
            "win_rate": None, "stop_rate": None, "stop_rate_low": None,
            "touch_rate": None, "worst_return": None, "p05_return": None,
            "verdict": "inconclusive",
        })
        return base

    rets = filled["ret_pct"].values
    n    = len(rets)
    mu   = float(np.mean(rets))
    se   = float(np.std(rets, ddof=1) / math.sqrt(n)) if n > 1 else math.nan
    tst  = mu / se if (math.isfinite(se) and se > 0) else math.nan

    if n < 100:
        verdict = "inconclusive"
    elif math.isfinite(tst) and tst >= 2.0:
        verdict = "confirmed"
    elif math.isfinite(tst) and tst <= -2.0:
        verdict = "rejected"
    else:
        verdict = "inconclusive"

    base.update({
        "expectancy_pct":   round(mu, 4),
        "expectancy_se":    round(se, 4) if math.isfinite(se) else None,
        "expectancy_tstat": round(tst,4) if math.isfinite(tst) else None,
        "win_rate":         round(float((rets>0).mean()),4),
        "stop_rate":        round(float(filled["stop_fired"].mean()),4),
        "stop_rate_low":    round(float(filled["stop_low_flag"].mean()),4),
        "touch_rate":       round(float(filled["touch_flag"].mean()),4),
        "worst_return":     round(float(np.min(rets)),4),
        "p05_return":       round(float(np.percentile(rets,5)),4),
        "verdict": verdict,
    })
    return base


def aggregate_results(trades: list, label: str) -> list:
    if not trades:
        return []
    df = pd.DataFrame(trades)
    records = []
    for t_str in ENTRY_TIMES:
        for k in K_VALUES:
            base = df[(df["entry_time"]==t_str) & (np.abs(df["k"]-k)<0.001)]
            if len(base) == 0:
                continue
            for split in ["all", "vix:<20", "vix:20-30", "vix:>30"]:
                if split == "all":
                    sub = base
                else:
                    sub = base[base["vix_band"]==split[4:]]
                rec = {"window": label, "entry_time": t_str, "k": float(k), "split": split}
                rec.update(cell_stats(sub, len(sub)))
                records.append(rec)
    return records


def compute_fog_widths(trades: list) -> dict:
    """
    Per-column fog width (% of max risk).
    net_bias = FOG_NET_SCALE × band_bias  (conservative short−long approx.)
    fog_width_pct = (net_bias × 100) / median_max_risk_of_filled_in_column
    """
    df = pd.DataFrame(trades)
    result = {}
    fallback_risk = (SPREAD_WIDTH - CREDIT_FLOOR) * MULTIPLIER
    for t_str in ENTRY_TIMES:
        band_bias = _FOG_BAND_BIAS.get(t_str, 1.70)
        net_bias  = FOG_NET_SCALE * band_bias
        filled = df[(df["entry_time"] == t_str) & (df["status"] == "filled")]
        med_risk = float(filled["max_risk"].median()) if len(filled) > 0 else fallback_risk
        fog_pct  = (net_bias * 100.0) / med_risk if med_risk > 0 else float("inf")
        result[t_str] = {
            "band_bias_usd":   round(band_bias, 4),
            "net_bias_usd":    round(net_bias,  4),
            "median_max_risk": round(med_risk,  2),
            "fog_width_pct":   round(fog_pct,   4),
        }
    return result


def find_positive_regions(records: list, fog_widths: dict | None = None) -> list:
    t_idx = {t: i for i, t in enumerate(ENTRY_TIMES)}
    k_idx = {float(k): j for j, k in enumerate(K_VALUES)}
    grid     = np.full((len(ENTRY_TIMES), N_K), np.nan)
    fog_grid = np.ones((len(ENTRY_TIMES), N_K), dtype=bool)   # True = fogged
    for r in records:
        if r["split"] == "all" and r.get("expectancy_tstat") is not None:
            i = t_idx.get(r["entry_time"])
            j = k_idx.get(round(float(r["k"]), 1))
            if i is not None and j is not None:
                grid[i, j] = r["expectancy_tstat"]
                if fog_widths:
                    fw  = fog_widths.get(r["entry_time"], {}).get("fog_width_pct", float("inf"))
                    exp = r.get("expectancy_pct") or 0.0
                    fog_grid[i, j] = abs(exp) <= fw
                else:
                    fog_grid[i, j] = False

    # Only READABLE cells (|exp| > fog_width) with t ≥ 2 count as positive
    mask = ((grid >= 2.0) & ~fog_grid).astype(int)
    seen = []
    hist = np.zeros(N_K, dtype=int)
    for r in range(len(ENTRY_TIMES)):
        hist = np.where(mask[r], hist+1, 0)
        stack: list = []
        for c in range(N_K+1):
            h  = int(hist[c]) if c < N_K else 0
            sc = c
            while stack and stack[-1][0] > h:
                sh, ssc = stack.pop()
                area = sh*(c-ssc)
                seen.append((area, r-sh+1, r, ssc, c-1))
                sc = ssc
            if h > 0:
                stack.append((h, sc))

    seen.sort(key=lambda x: -x[0])
    top3 = []
    for area, r1, r2, c1, c2 in seen[:50]:
        if area < 1: continue
        top3.append({
            "area_cells": int(area),
            "entry_time_range": [ENTRY_TIMES[r1], ENTRY_TIMES[r2]],
            "k_range": [float(K_VALUES[c1]), float(K_VALUES[c2])],
            "min_tstat": float(np.nanmin(grid[r1:r2+1, c1:c2+1])),
        })
        if len(top3) == 3: break
    return top3


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("="*70)
    print("0DTE Entry-Surface Backtest  —  Road A (VIXD model, fog bands)")
    print("="*70)

    import time, calendar
    t_start = time.time()

    # ── [1/6] Load SPX, VIX, VIXD ────────────────────────────────────────────
    print("\n[1/6] Loading SPX, VIX, VIXD bars …")
    spx = load_all_bars("spx")
    vix = load_all_bars("vix")
    vix_ts_us = vix.index.asi8
    vix_close = vix["Close"].values

    with open(DATA / "manifest.json") as mf:
        _manifest = json.load(mf)
    if "vixd" not in _manifest:
        months = []
        y, mo = 2023, 4
        while (y, mo) <= (2026, 5):
            months.append(f"{calendar.month_name[mo].upper()} {y}")
            mo += 1
            if mo > 12:
                mo = 1; y += 1
        raise RuntimeError(
            "HARD ERROR: 'vixd' key missing from trading_data/manifest.json.\n"
            f"Need {len(months)} months of 1-min VIXD history: {months[0]} … {months[-1]}.\n"
            "Download from Barchart.com, place in trading_data/, and add to manifest.json "
            "under key 'vixd' following the same pattern as 'spx' and 'vix'."
        )
    vixd = load_all_bars("vixd")
    vixd_ts_us = vixd.index.asi8
    vixd_close = vixd["Close"].values
    print(f"      SPX {len(spx):,}  VIX {len(vix):,}  VIXD {len(vixd):,}  ({time.time()-t_start:.1f}s)")

    # ── [2/6] Frozen calibration params ──────────────────────────────────────
    print("\n[2/6] Loading frozen calibration params …")
    calib_path = TOOLS / "calibration_report.json"
    with open(calib_path) as cf:
        calib_report = json.load(cf)
    p          = calib_report["params"]
    params     = (p["a0"], p["b"], p["c"])
    model_form = calib_report.get("model_form", "abs_m")
    use_abs_m  = (model_form == "abs_m")
    print(f"      [FROZEN from {calib_path.name}]  "
          f"a0={params[0]:.4f}  b={params[1]:.4f}  c={params[2]:.4f}  form={model_form}")
    gate_pct = calib_report.get("gate_median_abs_pct_err", float("nan"))
    print(f"      Calibration gate (px≥$0.50): {gate_pct*100:.1f}%  "
          f"[{'PASS' if gate_pct <= 0.35 else 'FAIL — Road A bias bands apply'}]")
    if calib_report.get("biased_days"):
        print(f"      Biased days: {calib_report['biased_days']}")

    # ── [3/6] Trading days + VIXD coverage check ─────────────────────────────
    print("\n[3/6] Identifying trading days and validating VIXD coverage …")
    trading_days = []
    for d in sorted(set(spx.index.date)):
        try:
            rth = spx.loc[str(d)].between_time("09:30", "16:05")
        except KeyError:
            continue
        if len(rth) >= 10:
            trading_days.append(pd.Timestamp(str(d), tz="America/New_York"))

    is_days = [d for d in trading_days if IS_START <= d <= IN_SAMPLE_END]
    ho_days = [d for d in trading_days if HOLDOUT_START <= d <= HOLDOUT_END]

    vixd_dates = set(vixd.index.date)
    missing_vixd = [d for d in is_days + ho_days if d.date() not in vixd_dates]
    if missing_vixd:
        raise RuntimeError(
            f"HARD ERROR: VIXD missing for {len(missing_vixd)} trading days. "
            f"First: {missing_vixd[0].date()}  Last: {missing_vixd[-1].date()}"
        )
    print(f"      In-sample: {len(is_days)} days ({IS_START.date()} → {IN_SAMPLE_END.date()})")
    print(f"      Holdout:   {len(ho_days)} days ({HOLDOUT_START.date()} → {HOLDOUT_END.date()})")
    print(f"      VIXD coverage: OK ({len(vixd_dates)} dates loaded)")

    # ── [4/6] Backtest ────────────────────────────────────────────────────────
    print("\n[4/6] Running backtest …")
    stale_cnt    = 0
    skipped_rows: list = []
    all_is: list = []
    all_ho: list = []
    t2 = time.time()

    for label, days, trades in [("in_sample", is_days, all_is), ("holdout", ho_days, all_ho)]:
        for i, date in enumerate(days):
            if (i+1) % 250 == 0:
                print(f"      {label}: {i+1}/{len(days)}  {time.time()-t2:.0f}s", flush=True)
            try:
                day_spx = spx.loc[str(date.date())]
                if isinstance(day_spx, pd.Series):
                    day_spx = day_spx.to_frame().T
            except KeyError:
                continue
            day_trades = simulate_day(
                date, day_spx,
                vix_ts_us, vix_close,
                vixd_ts_us, vixd_close,
                params, skipped_rows, use_abs_m,
            )
            trades.extend(day_trades)
            stale_cnt += sum(1 for r in day_trades if r.get("stale_entry"))

    n_is_f = sum(1 for t in all_is if t["status"]=="filled")
    n_is_s = sum(1 for t in all_is if t["status"]=="skipped")
    n_ho_f = sum(1 for t in all_ho if t["status"]=="filled")
    n_ho_s = sum(1 for t in all_ho if t["status"]=="skipped")
    print(f"      IS filled={n_is_f:,}  skipped={n_is_s:,}")
    print(f"      HO filled={n_ho_f:,}  skipped={n_ho_s:,}")
    print(f"      Stale entry bars: {stale_cnt}  ({time.time()-t2:.1f}s)")

    # ── [5/6] Fog bands + record annotation ──────────────────────────────────
    print("\n[5/6] Computing fog widths and annotating records …")
    fog_widths = compute_fog_widths(all_is)
    is_recs    = aggregate_results(all_is, "in_sample")
    for rec in is_recs:
        if rec["split"] == "all":
            fw  = fog_widths.get(rec["entry_time"], {}).get("fog_width_pct", float("inf"))
            exp = rec.get("expectancy_pct") or 0.0
            rec["fog_width_pct"] = round(fw, 4)
            rec["readable"]      = rec.get("expectancy_pct") is not None and abs(exp) > fw
    ho_recs = aggregate_results(all_ho, "holdout")

    # ── [6/6] Write outputs ───────────────────────────────────────────────────
    print("\n[6/6] Writing outputs …")
    skip_path = TOOLS / "skipped_log.csv"
    (pd.DataFrame(skipped_rows) if skipped_rows else
     pd.DataFrame(columns=["date","entry_time","k","K_short","K_long","credit","vix"])
    ).to_csv(skip_path, index=False)
    print(f"      → {skip_path.relative_to(REPO)}")

    fog_meta = {t: {k: v for k, v in d.items()} for t, d in fog_widths.items()}
    out_path = TOOLS / "results.json"
    with open(out_path, "w") as f:
        json.dump({
            "metadata": {
                "road": "A",
                "window": "in_sample",
                "start": str(IS_START.date()),
                "end":   str(IN_SAMPLE_END.date()),
                "n_trading_days": len(is_days),
                "n_filled": n_is_f, "n_skipped": n_is_s,
                "stale_entry_bars": stale_cnt,
                "calib_params": calib_report["params"],
                "calib_model_form": model_form,
                "calib_gate_pct": round(gate_pct, 4),
                "fog_widths": fog_meta,
                "fog_net_scale_approx": FOG_NET_SCALE,
            },
            "records": is_recs,
        }, f, indent=2, default=str)
    print(f"      → {out_path.relative_to(REPO)}")

    ho_path = TOOLS / "holdout_results.json"
    with open(ho_path, "w") as f:
        json.dump({
            "metadata": {
                "road": "A",
                "window": "holdout",
                "start": str(HOLDOUT_START.date()),
                "end":   str(HOLDOUT_END.date()),
                "n_trading_days": len(ho_days),
                "n_filled": n_ho_f, "n_skipped": n_ho_s,
                "calib_params": calib_report["params"],
                "fog_widths": fog_meta,
            },
            "records": ho_recs,
        }, f, indent=2, default=str)
    print(f"      → {ho_path.relative_to(REPO)}")

    # ── Console summary ───────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("ROAD A  IN-SAMPLE SUMMARY")
    print("="*70)
    print(f"Window:  {IS_START.date()} → {IN_SAMPLE_END.date()}  ({len(is_days)} days)")
    print(f"Pricing: IV = (VIXD/100) × max(a0 + b·m + c·|m|, 0.05)  [frozen params]")
    print(f"         a0={params[0]:.4f}  b={params[1]:.4f}  c={params[2]:.4f}")
    print(f"         Calibration gate: {gate_pct*100:.1f}%  [bias bands applied per spec]")
    print(f"Trades:  {n_is_f:,} filled  {n_is_s:,} skipped  {stale_cnt} stale-bar entries")
    print()
    print(f"{'Entry':<8}  {'fog_w%':>7}  {'net_bias$':>9}  {'readable':>9}  {'fogged':>7}")
    print("-"*50)
    for t_str in ENTRY_TIMES:
        fw   = fog_widths[t_str]
        t_recs = [r for r in is_recs if r["entry_time"]==t_str and r["split"]=="all"]
        n_rd = sum(1 for r in t_recs if r.get("readable"))
        n_fg = len(t_recs) - n_rd
        print(f"{t_str:<8}  {fw['fog_width_pct']:>7.2f}%  "
              f"${fw['net_bias_usd']:>8.3f}  {n_rd:>9}  {n_fg:>7}")

    regions = find_positive_regions(is_recs, fog_widths)
    print()
    if regions:
        print(f"Contiguous READABLE positive regions (t≥2, |exp|>fog, ≥3 cells):")
        for i, reg in enumerate(regions, 1):
            print(f"  {i}. T={reg['entry_time_range'][0]}–{reg['entry_time_range'][1]}"
                  f"  k={reg['k_range'][0]}–{reg['k_range'][1]}"
                  f"  area={reg['area_cells']}  min_t={reg['min_tstat']:.2f}")
    else:
        n_readable_pos = sum(
            1 for r in is_recs
            if r["split"]=="all" and r.get("readable") and (r.get("expectancy_pct") or 0) > 0
        )
        if n_readable_pos == 0:
            print("RESULT: Entire surface is FOGGED or flat/negative at this error level.")
            print("        Model cannot resolve an edge; real option-chain data required.")
        else:
            print(f"RESULT: {n_readable_pos} isolated READABLE positive cells found,")
            print("        but no contiguous region ≥3 cells — flagged as noise.")
    print(f"\nTotal elapsed: {time.time()-t_start:.1f}s")
    print("="*70)


if __name__ == "__main__":
    main()
