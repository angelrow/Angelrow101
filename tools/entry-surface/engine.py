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
ERA_SPLIT_DATE   = pd.Timestamp("2022-05-16", tz="America/New_York")
IN_SAMPLE_END    = pd.Timestamp("2024-05-31", tz="America/New_York")
HOLDOUT_START    = pd.Timestamp("2024-06-01", tz="America/New_York")
STALE_ENTRY_WINDOW = 5   # minutes

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
    3-parameter model: IV = (VIX/100) × max(a0 + b·m + c·m², 0.05),  c ≥ 0.
    VIX enters only through sigma_base = VIX/100; no separate VIX-level term.
    Returns (params, model_form, report).  params = (a0, b, c).
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

    S_arr     = spx_cl[spx_idx]
    vix_arr   = vix_cl[vix_idx]
    sigma_arr = vix_arr / 100.0
    K_arr     = calib["strike"].values.astype(float)
    px_arr    = calib["price_last"].values.astype(float)
    vol_arr   = pd.to_numeric(calib["volume"], errors="coerce").fillna(0).values

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

    # ── Step 2: WLS initial guess (3-param: a0, b, c) ─────────────────────────
    # Bounds: a0 ∈ [-5,5], b ∈ [-10,10], c ∈ [0,5]
    BOUNDS_LO = np.array([-5., -10., 0.])
    BOUNDS_HI = np.array([ 5.,  10., 5.])

    valid_iv = np.isfinite(iv_market) & (sigma_arr > 0)
    if valid_iv.sum() >= 3:
        Y  = iv_market[valid_iv] / sigma_arr[valid_iv]
        Xm = m_arr[valid_iv]
        Wv = w_arr[valid_iv]
        # Quadratic basis: [1, m, m²]
        Xq = np.column_stack([np.ones(valid_iv.sum()), Xm, Xm**2])
        # |m| basis:        [1, m, |m|]
        Xa = np.column_stack([np.ones(valid_iv.sum()), Xm, np.abs(Xm)])
        x0_quad, _, _, _ = np.linalg.lstsq(Xq * Wv[:,None], Y * Wv, rcond=None)
        x0_abs,  _, _, _ = np.linalg.lstsq(Xa * Wv[:,None], Y * Wv, rcond=None)
        x0_quad = np.clip(x0_quad, BOUNDS_LO, BOUNDS_HI)
        x0_abs  = np.clip(x0_abs,  BOUNDS_LO, BOUNDS_HI)
    else:
        x0_quad = x0_abs = np.array([1.0, -0.3, 0.1])

    # ── Step 3a: fit c·m² (c ≥ 0) ────────────────────────────────────────────
    def residuals_quad(p):
        iv = sigma_arr * np.maximum(p[0] + p[1]*m_arr + p[2]*m_arr**2, 0.05)
        return w_arr * (_bsm_put_arr(S_arr, K_arr, T_rem_arr, RISK_FREE, DIV_YIELD, iv) - px_arr)

    res_quad = least_squares(residuals_quad, x0_quad, method="trf",
                             bounds=(BOUNDS_LO.tolist(), BOUNDS_HI.tolist()),
                             max_nfev=20000)
    pq   = res_quad.x
    iv_q = sigma_arr * np.maximum(pq[0] + pq[1]*m_arr + pq[2]*m_arr**2, 0.05)
    px_q = _bsm_put_arr(S_arr, K_arr, T_rem_arr, RISK_FREE, DIV_YIELD, iv_q)
    sr_q = px_q - px_arr
    mae_q = float(np.mean(np.abs(sr_q)))

    abs_m_f    = np.abs(m_arr)
    band_edges = [(0, 1), (1, 2), (2, 3)]
    bias_q = [
        float(np.mean(sr_q[(abs_m_f >= lo) & (abs_m_f < hi)]))
        if ((abs_m_f >= lo) & (abs_m_f < hi)).sum() > 0 else np.nan
        for lo, hi in band_edges
    ]

    # Systematic curvature: all-same-sign or strictly monotone across bands
    vb = [b for b in bias_q if not np.isnan(b)]
    systematic = (
        len(vb) >= 2 and (
            all(b > 0 for b in vb) or
            all(b < 0 for b in vb) or
            all(vb[i] < vb[i+1] for i in range(len(vb)-1)) or
            all(vb[i] > vb[i+1] for i in range(len(vb)-1))
        )
    )

    # ── Step 3b: try c·|m| if systematic curvature ────────────────────────────
    mae_abs_m = None
    bias_abs  = [np.nan, np.nan, np.nan]

    if systematic:
        def residuals_abs(p):
            iv = sigma_arr * np.maximum(p[0] + p[1]*m_arr + p[2]*np.abs(m_arr), 0.05)
            return w_arr * (_bsm_put_arr(S_arr, K_arr, T_rem_arr, RISK_FREE, DIV_YIELD, iv) - px_arr)

        res_abs = least_squares(residuals_abs, x0_abs, method="trf",
                                bounds=(BOUNDS_LO.tolist(), BOUNDS_HI.tolist()),
                                max_nfev=20000)
        pa   = res_abs.x
        iv_a = sigma_arr * np.maximum(pa[0] + pa[1]*m_arr + pa[2]*np.abs(m_arr), 0.05)
        px_a = _bsm_put_arr(S_arr, K_arr, T_rem_arr, RISK_FREE, DIV_YIELD, iv_a)
        sr_a = px_a - px_arr
        mae_abs_m = float(np.mean(np.abs(sr_a)))
        bias_abs = [
            float(np.mean(sr_a[(abs_m_f >= lo) & (abs_m_f < hi)]))
            if ((abs_m_f >= lo) & (abs_m_f < hi)).sum() > 0 else np.nan
            for lo, hi in band_edges
        ]

    use_abs_m  = systematic and (mae_abs_m is not None) and (mae_abs_m < mae_q)
    model_form = "abs_m" if use_abs_m else "m2"
    p_chosen   = res_abs.x if use_abs_m else pq   # type: ignore[possibly-undefined]
    params     = tuple(p_chosen)

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

    # Per |m| band: same metrics + signed median
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

    overall_mae     = float(np.mean(abs_err))
    overall_med_pct = float(np.nanmedian(pct_err))

    # Any-day bias flag
    biased_days = [exp for exp, d in per_expiry.items() if d["bias_flag"]]

    report = {
        "params": {"a0": params[0], "b": params[1], "c": params[2]},
        "model_form": model_form,
        "n_calibration_prints": int(len(px_arr)),
        "n_gate_prints": int(gate_mask.sum()),
        "overall_mae": overall_mae,
        "overall_median_abs_pct_err": overall_med_pct,
        "gate_median_abs_pct_err": gate_med_pct,
        "systematic_curvature_detected": systematic,
        "per_band_bias_m2":    {f"|m| [{lo},{hi})": b for (lo,hi),b in zip(band_edges, bias_q)},
        "per_band_bias_abs_m": {f"|m| [{lo},{hi})": b for (lo,hi),b in zip(band_edges, bias_abs)},
        "mae_m2":    mae_q,
        "mae_abs_m": mae_abs_m,
        "per_expiry": per_expiry,
        "per_m_band": per_band,
        "biased_days": biased_days,
        "warning_unreliable": gate_med_pct > 0.35,
    }
    return params, model_form, report


# ── Vectorised day simulation ─────────────────────────────────────────────────

def simulate_day(
    date: pd.Timestamp,
    day_spx: pd.DataFrame,
    vix_ts_us: np.ndarray,   # full VIX index in μs (from vix.index.asi8)
    vix_close: np.ndarray,
    params: tuple,
    skipped_rows: list,
    use_abs_m: bool = False,
) -> list:
    """
    Simulate all (T, k) cells for one trading day.
    LOOK-AHEAD GUARD: all VIX lookups use at-or-before the SPX bar timestamp.
    """
    settle_ts = settlement_time(day_spx)
    if settle_ts is None:
        return []

    settle_us  = _ts_to_us(settle_ts)   # μs — consistent with .asi8
    tz         = settle_ts.tzinfo
    spx_us     = day_spx.index.asi8      # μs
    spx_close  = day_spx["Close"].values
    spx_low    = day_spx["Low"].values

    era_val  = "post" if date >= ERA_SPLIT_DATE else "pre"
    results: list = []

    for t_str in ENTRY_TIMES:
        h, m = int(t_str[:2]), int(t_str[3:])
        entry_us = _ts_to_us(
            pd.Timestamp(date.year, date.month, date.day, h, m, tzinfo=tz)
        )

        if entry_us >= settle_us:
            continue

        # SPX entry bar at-or-before entry_us, within stale window
        idx_e = int(np.searchsorted(spx_us, entry_us, side="right")) - 1
        if idx_e < 0:
            continue
        actual_us  = int(spx_us[idx_e])
        stale_mins = (entry_us - actual_us) / US_PER_MIN
        if stale_mins > STALE_ENTRY_WINDOW:
            continue
        is_stale = stale_mins > 0.5

        S = float(spx_close[idx_e])

        # VIX at entry (LOOK-AHEAD GUARD: use actual_us, never a later timestamp)
        # LOOK-AHEAD GUARD LINE: vix lookup uses actual_us (SPX bar timestamp), not entry_us
        vi_e = int(np.searchsorted(vix_ts_us, actual_us, side="right")) - 1
        if vi_e < 0:
            continue
        vix_val  = float(vix_close[vi_e])
        sigma_b  = vix_val / 100.0
        vix_sc_e = vix_val / 20.0

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
        vix_band    = "<20" if vix_val < 20 else ("20-30" if vix_val <= 30 else ">30")

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
                    "era": era_val, "vix_band": vix_band,
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

        # VIX for each post-entry bar (vectorised searchsorted)
        # LOOK-AHEAD GUARD LINE: vix lookup uses post_us (SPX bar timestamps), never later
        vi = np.searchsorted(vix_ts_us, post_us, side="right") - 1
        vi = np.maximum(vi, 0)
        pv   = vix_close[vi]                                # (n_bars,)
        psig = pv / 100.0
        pvsc = pv / 20.0
        pT   = np.maximum((settle_us - post_us) / US_PER_MIN, 0.0) / (252*390)

        # Broadcast: (n_bars,1) × (1,N_K) → (n_bars,N_K)
        S_cl   = post_cl[:,None]
        S_lo   = post_lo[:,None]
        sig_t  = psig[:,None]
        vsc_t  = pvsc[:,None]
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
                    "era": era_val, "vix_band": vix_band,
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
            for split in ["all","era:pre","era:post","vix:<20","vix:20-30","vix:>30"]:
                if split == "all":
                    sub = base
                elif split.startswith("era:"):
                    sub = base[base["era"]==split[4:]]
                else:
                    sub = base[base["vix_band"]==split[4:]]
                rec = {"window": label, "entry_time": t_str, "k": float(k), "split": split}
                rec.update(cell_stats(sub, len(sub)))
                records.append(rec)
    return records


def find_positive_regions(records: list) -> list:
    t_idx = {t: i for i, t in enumerate(ENTRY_TIMES)}
    k_idx = {float(k): j for j, k in enumerate(K_VALUES)}
    grid  = np.full((len(ENTRY_TIMES), N_K), np.nan)
    for r in records:
        if r["split"]=="all" and r.get("expectancy_tstat") is not None:
            i = t_idx.get(r["entry_time"])
            j = k_idx.get(round(float(r["k"]),1))
            if i is not None and j is not None:
                grid[i, j] = r["expectancy_tstat"]

    mask = (grid >= 2.0).astype(int)
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
    print("0DTE Entry-Surface Backtest Engine")
    print("="*70)

    import time
    t_start = time.time()

    print("\n[1/5] Loading SPX and VIX bars …")
    spx = load_all_bars("spx")
    vix = load_all_bars("vix")
    vix_ts_us = vix.index.asi8    # μs — pre-extracted for fast searchsorted
    vix_close = vix["Close"].values
    print(f"      SPX {len(spx):,}  VIX {len(vix):,}  ({time.time()-t_start:.1f}s)")

    print("\n[2/5] Running calibration …")
    t1 = time.time()
    params, model_form, calib_report = run_calibration(spx, vix)
    use_abs_m = (model_form == "abs_m")
    p = calib_report["params"]
    gate_pct = calib_report["gate_median_abs_pct_err"]
    print(f"      model={model_form}  a0={p['a0']:.4f}  b={p['b']:.4f}  c={p['c']:.4f}")
    print(f"      MAE=${calib_report['overall_mae']:.4f}   "
          f"gate median|%err|={gate_pct*100:.2f}%  ({time.time()-t1:.1f}s)")
    if calib_report["warning_unreliable"]:
        print(f"\n  *** WARNING: gate median |%err| {gate_pct*100:.1f}% > 35% — "
              "expectancy outputs may be unreliable ***\n")
    if calib_report.get("biased_days"):
        for bd in calib_report["biased_days"]:
            d = calib_report["per_expiry"][bd]
            print(f"  *** BIAS FLAG: {bd}  signed_median=${d['signed_median_err']:.2f} ***\n")
    calib_path = TOOLS / "calibration_report.json"
    with open(calib_path, "w") as f:
        json.dump(calib_report, f, indent=2)
    print(f"      → {calib_path.relative_to(REPO)}")

    print("\n[3/5] Identifying trading days …")
    trading_days = []
    for d in sorted(set(spx.index.date)):
        try:
            rth = spx.loc[str(d)].between_time("09:30", "16:05")
        except KeyError:
            continue
        if len(rth) >= 10:
            trading_days.append(pd.Timestamp(str(d), tz="America/New_York"))

    is_days = [d for d in trading_days
               if pd.Timestamp("2016-01-04", tz="America/New_York") <= d <= IN_SAMPLE_END]
    ho_days = [d for d in trading_days
               if HOLDOUT_START <= d <= pd.Timestamp("2026-05-29", tz="America/New_York")]
    print(f"      In-sample: {len(is_days)}   Holdout: {len(ho_days)}")

    print("\n[4/5] Running backtest …")
    stale_cnt = 0
    skipped_rows: list = []
    all_is: list = []
    all_ho: list = []
    t2 = time.time()

    for label, days, trades in [("in_sample", is_days, all_is), ("holdout", ho_days, all_ho)]:
        for i, date in enumerate(days):
            if (i+1) % 250 == 0:
                elapsed = time.time()-t2
                print(f"      {label}: {i+1}/{len(days)}  {elapsed:.0f}s elapsed", flush=True)
            try:
                day_spx = spx.loc[str(date.date())]
                if isinstance(day_spx, pd.Series):
                    day_spx = day_spx.to_frame().T
            except KeyError:
                continue
            day_trades = simulate_day(date, day_spx, vix_ts_us, vix_close, params, skipped_rows, use_abs_m)
            trades.extend(day_trades)
            stale_cnt += sum(1 for r in day_trades if r.get("stale_entry"))

    n_is_f = sum(1 for t in all_is if t["status"]=="filled")
    n_is_s = sum(1 for t in all_is if t["status"]=="skipped")
    n_ho_f = sum(1 for t in all_ho if t["status"]=="filled")
    n_ho_s = sum(1 for t in all_ho if t["status"]=="skipped")
    print(f"      IS filled={n_is_f:,}  skipped={n_is_s:,}")
    print(f"      HO filled={n_ho_f:,}  skipped={n_ho_s:,}")
    print(f"      Stale entry bars: {stale_cnt}  ({time.time()-t2:.1f}s)")

    print("\n[5/5] Writing outputs …")
    skip_path = TOOLS / "skipped_log.csv"
    (pd.DataFrame(skipped_rows) if skipped_rows else
     pd.DataFrame(columns=["date","entry_time","k","K_short","K_long","credit","vix"])
    ).to_csv(skip_path, index=False)
    print(f"      → {skip_path.relative_to(REPO)}")

    is_recs = aggregate_results(all_is, "in_sample")
    out_path = TOOLS / "results.json"
    with open(out_path, "w") as f:
        json.dump({
            "metadata": {
                "window": "in_sample", "start": "2016-01-04",
                "end": str(IN_SAMPLE_END.date()),
                "n_trading_days": len(is_days),
                "n_filled": n_is_f, "n_skipped": n_is_s,
                "stale_entry_bars": stale_cnt,
                "calib_params": calib_report["params"],
                "calib_model_form": model_form,
                "calib_mae": calib_report["overall_mae"],
                "calib_median_abs_pct_err": med_pct,
            },
            "records": is_recs,
        }, f, indent=2, default=str)
    print(f"      → {out_path.relative_to(REPO)}")

    ho_recs = aggregate_results(all_ho, "holdout")
    ho_path = TOOLS / "holdout_results.json"
    with open(ho_path, "w") as f:
        json.dump({
            "metadata": {
                "window": "holdout",
                "start": str(HOLDOUT_START.date()), "end": "2026-05-29",
                "n_trading_days": len(ho_days),
                "n_filled": n_ho_f, "n_skipped": n_ho_s,
                "calib_params": calib_report["params"],
            },
            "records": ho_recs,
        }, f, indent=2, default=str)
    print(f"      → {ho_path.relative_to(REPO)}")

    print("\n"+"="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Calibration  MAE=${calib_report['overall_mae']:.4f}  "
          f"median|%err|={med_pct*100:.2f}%")
    if calib_report["warning_unreliable"]:
        print("  *** WARNING: median |% err| > 35% — outputs may be unreliable ***")
    print(f"In-sample:  {n_is_f:,} filled,  {n_is_s:,} skipped")
    print(f"Holdout:    {n_ho_f:,} filled,  {n_ho_s:,} skipped")
    regions = find_positive_regions(is_recs)
    if regions:
        print(f"\nTop {len(regions)} contiguous positive-expectancy regions (t≥2, in-sample):")
        for i, reg in enumerate(regions, 1):
            print(f"  {i}. T={reg['entry_time_range'][0]}–{reg['entry_time_range'][1]}"
                  f"  k={reg['k_range'][0]}–{reg['k_range'][1]}"
                  f"  area={reg['area_cells']}  min_t={reg['min_tstat']:.2f}")
    else:
        print("\nNo contiguous positive-expectancy regions found (t≥2) in-sample.")
    print(f"\nTotal elapsed: {time.time()-t_start:.1f}s")
    print("="*70)


if __name__ == "__main__":
    main()
