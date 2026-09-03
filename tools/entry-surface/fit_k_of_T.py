"""
VIXD intraday conversion factor k(T) + option recalibration.

Mechanism under test: VIXD is a 24-hour, 365-day-annualised index, but the EM
formula treats it as a 390-minute, 252-day quantity.  The conversion therefore
mis-scales, worst near midday when VIXD's forward window is mostly tomorrow's
session.  This script fits an explicit intraday conversion factor k(T),
verifies it OUT OF SAMPLE, and only then re-runs the four-day option
calibration with the corrected sigma_base.

Stage 1  fit k(T)          2023-04-24 -> 2025-05-31
Stage 2  verify k(T)       2025-06-01 -> last date all three series exist
Stage 3  recalibrate       four expiry days, refit (a0, b, c) only

k(T) is fitted ONCE on the fit window and frozen.  No refitting on the test
window.  No backtest.  No surface.  No fog bands.
"""

import json
import math
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline

from engine import TOOLS, REPO, load_all_bars, run_calibration
from vixd_validation import (
    ENTRY_TIMES_VAL, load_vixd_1min, build_observations, summarise,
)

# ── Windows ───────────────────────────────────────────────────────────────────
FIT_START  = "2023-04-24"
FIT_END    = "2025-05-31"
TEST_START = "2025-06-01"
# TEST_END is automatic: the last date where SPX, VIX and VIXD all exist.

# ── Constants ─────────────────────────────────────────────────────────────────
K_DIVISOR       = 0.6745    # median of |N(0,1)| — used to define k(T)
Z_TARGET        = 0.67      # display target per the pre-committed spec
COV1_T, COV2_T  = 0.68, 0.95
TAIL3_T         = 0.003

K_MIN_SANE, K_MAX_SANE = 0.4, 1.2          # Stage-1 hard bounds

S2_OVERALL_LO, S2_OVERALL_HI = 0.62, 0.74  # Stage-2 pass bounds
S2_TIME_LO,    S2_TIME_HI    = 0.58, 0.78
S2_REGIME_LO,  S2_REGIME_HI  = 0.60, 0.76

S3_GATE_PASS,  S3_GATE_PARTIAL = 0.35, 0.50
S3_BIAS_LIMIT                  = 1.00

REGIMES  = ["<20", "20-30", ">30"]
OUT_K    = TOOLS / "k_of_T.json"
OUT_CAL  = TOOLS / "calibration_report.json"
PREV_CAL = TOOLS / "calibration_report_prev.json"


def _mod(tstr: str) -> int:
    """'HH:MM' -> minute of day."""
    return int(tstr[:2]) * 60 + int(tstr[3:])


def _f(x, nd=6):
    if x is None:
        return None
    x = float(x)
    return None if not math.isfinite(x) else round(x, nd)


def _pct(x):
    return "   n/a" if x is None else f"{x*100:5.1f}%"


# ── Smooth k(T) ───────────────────────────────────────────────────────────────

def make_k_fn(x_knots, y_knots):
    """Natural cubic spline through the 13 fitted points.

    CHOICE (stated): natural cubic spline, not piecewise-linear — it keeps
    sigma_base continuous AND smooth through the day, so neighbouring minutes
    never see a kink in the conversion factor.

    Outside the knot range (before 09:35 / after 15:30) the curve is CLAMPED to
    the endpoint value.  Cubic extrapolation beyond knots is unstable and would
    be an unstated assumption; clamping is explicit and bounded.  Clamp counts
    are tracked and reported.
    """
    cs = CubicSpline(x_knots, y_knots, bc_type="natural")
    lo, hi = float(x_knots[0]), float(x_knots[-1])

    def k_fn(mod):
        mod = np.asarray(mod, dtype=float)
        k_fn.n_clamp_lo += int((mod < lo).sum())
        k_fn.n_clamp_hi += int((mod > hi).sum())
        return cs(np.clip(mod, lo, hi))

    k_fn.n_clamp_lo = 0
    k_fn.n_clamp_hi = 0
    k_fn.spline = cs
    k_fn.lo, k_fn.hi = lo, hi
    return k_fn


# ── Stage 1 ───────────────────────────────────────────────────────────────────

def stage1(obs: pd.DataFrame) -> tuple:
    print("\n" + "=" * 78)
    print("STAGE 1 — FIT k(T)   window " + FIT_START + " -> " + FIT_END)
    print("=" * 78)

    fit = obs[(obs["date"] >= FIT_START) & (obs["date"] <= FIT_END)]
    if len(fit) == 0:
        raise RuntimeError("HARD ERROR: no observations in the fit window.")

    print(f"  observations: {len(fit):,}   "
          f"days: {fit['date'].nunique():,}   "
          f"({fit['date'].min()} -> {fit['date'].max()})")
    print(f"\n  k(T) = median(z_vixd) / {K_DIVISOR}")
    print(f"  {'T':<8} {'n':>6} {'median z':>10} {'k(T)':>9}")
    print("  " + "-" * 36)

    pts = []
    for t in ENTRY_TIMES_VAL:
        sub = fit[fit["entry_time"] == t]["z_vixd"].to_numpy()
        sub = sub[np.isfinite(sub)]
        if len(sub) == 0:
            raise RuntimeError(f"HARD ERROR: no fit-window observations at entry time {t}.")
        med = float(np.median(sub))
        k   = med / K_DIVISOR
        pts.append({"entry_time": t, "minute_of_day": _mod(t),
                    "n": int(len(sub)), "median_z": _f(med), "k": _f(k)})
        print(f"  {t:<8} {len(sub):>6,} {med:>10.4f} {k:>9.4f}")

    ks = np.array([p["k"] for p in pts], dtype=float)
    bad = [(p["entry_time"], p["k"]) for p in pts
           if not (K_MIN_SANE <= p["k"] <= K_MAX_SANE)]
    if bad:
        raise RuntimeError(
            "HARD ERROR: k(T) outside sanity bounds "
            f"[{K_MIN_SANE}, {K_MAX_SANE}] at: "
            + ", ".join(f"{t} k={k:.4f}" for t, k in bad)
        )
    print(f"\n  sanity: all 13 points within [{K_MIN_SANE}, {K_MAX_SANE}]  OK")
    print(f"  raw k range: {ks.min():.4f} … {ks.max():.4f}")

    x = np.array([p["minute_of_day"] for p in pts], dtype=float)
    k_fn = make_k_fn(x, ks)

    # Sanity-check the SMOOTH curve across the whole RTH grid, not just knots.
    grid = np.arange(_mod("09:30"), _mod("16:00") + 1, dtype=float)
    kg = k_fn(grid)
    k_fn.n_clamp_lo = k_fn.n_clamp_hi = 0     # reset after the probe
    if kg.min() < K_MIN_SANE or kg.max() > K_MAX_SANE:
        raise RuntimeError(
            f"HARD ERROR: smoothed k(T) leaves [{K_MIN_SANE}, {K_MAX_SANE}] "
            f"(min {kg.min():.4f}, max {kg.max():.4f}) on the 09:30–16:00 grid."
        )
    print(f"  smooth curve (natural cubic spline) on 09:30–16:00 grid: "
          f"{kg.min():.4f} … {kg.max():.4f}  OK")
    print("  NOTE: outside 09:35–15:30 the curve is CLAMPED to the endpoint value.")

    return pts, k_fn


# ── Stage 2 ───────────────────────────────────────────────────────────────────

def stage2(obs: pd.DataFrame, k_fn) -> tuple:
    print("\n" + "=" * 78)
    print("STAGE 2 — VERIFY k(T) OUT OF SAMPLE   window " + TEST_START + " -> auto")
    print("=" * 78)

    te = obs[obs["date"] >= TEST_START].copy()
    if len(te) == 0:
        raise RuntimeError("HARD ERROR: no observations in the test window.")

    kv = k_fn(te["entry_time"].map(_mod).to_numpy(dtype=float))
    te["k"] = kv
    # EM_corrected = EM x k   =>   z_corrected = z / k
    te["z_vixd_corr"] = te["z_vixd"] / te["k"]

    print(f"  observations: {len(te):,}   "
          f"days: {te['date'].nunique():,}   "
          f"({te['date'].min()} -> {te['date'].max()})")
    print("  k(T) FROZEN from Stage 1 — not refitted here.")

    def _trio(frame):
        return {
            "vixd_corrected": summarise(frame["z_vixd_corr"].to_numpy()),
            "vixd_raw":       summarise(frame["z_vixd"].to_numpy()),
            "vix":            summarise(frame["z_vix"].to_numpy()),
        }

    overall = _trio(te)
    per_t   = {t: _trio(te[te["entry_time"] == t]) for t in ENTRY_TIMES_VAL}
    per_r   = {r: _trio(te[te["regime"] == r]) for r in REGIMES}

    print("\n  OVERALL (test window)")
    print(f"  {'input':<16} {'n':>7} {'med z':>8} {'1σ cov':>8} {'2σ cov':>8} {'>3σ':>7}")
    for lbl, key in (("VIXD corrected", "vixd_corrected"),
                     ("VIXD raw", "vixd_raw"), ("VIX", "vix")):
        s = overall[key]
        print(f"  {lbl:<16} {s['n']:>7,} {s['median_z']:>8.3f} "
              f"{_pct(s['cov_1sig']):>8} {_pct(s['cov_2sig']):>8} {_pct(s['tail_3sig']):>7}")
    print(f"  {'target':<16} {'':>7} {Z_TARGET:>8.3f} {COV1_T*100:>7.1f}% "
          f"{COV2_T*100:>7.1f}% {TAIL3_T*100:>6.1f}%")

    print("\n  PER ENTRY TIME — 1σ coverage (corrected | raw | VIX), median z corrected")
    print(f"  {'T':<8} {'n':>6} {'corr 1σ':>9} {'raw 1σ':>8} {'VIX 1σ':>8} "
          f"{'corr medz':>10} {'corr 2σ':>9}")
    for t in ENTRY_TIMES_VAL:
        c, r, v = per_t[t]["vixd_corrected"], per_t[t]["vixd_raw"], per_t[t]["vix"]
        print(f"  {t:<8} {c['n']:>6,} {_pct(c['cov_1sig']):>9} {_pct(r['cov_1sig']):>8} "
              f"{_pct(v['cov_1sig']):>8} {c['median_z']:>10.3f} {_pct(c['cov_2sig']):>9}")

    print("\n  PER VIX REGIME — 1σ coverage (corrected | raw | VIX), median z corrected")
    print(f"  {'regime':<8} {'n':>6} {'corr 1σ':>9} {'raw 1σ':>8} {'VIX 1σ':>8} "
          f"{'corr medz':>10} {'corr 2σ':>9}")
    for r_ in REGIMES:
        c, r, v = per_r[r_]["vixd_corrected"], per_r[r_]["vixd_raw"], per_r[r_]["vix"]
        if c["n"] == 0:
            print(f"  {r_:<8} {0:>6} (no observations)")
            continue
        print(f"  {r_:<8} {c['n']:>6,} {_pct(c['cov_1sig']):>9} {_pct(r['cov_1sig']):>8} "
              f"{_pct(v['cov_1sig']):>8} {c['median_z']:>10.3f} {_pct(c['cov_2sig']):>9}")

    # ── Pass rule (pre-committed) ──
    fails = []
    o1 = overall["vixd_corrected"]["cov_1sig"]
    if not (S2_OVERALL_LO <= o1 <= S2_OVERALL_HI):
        fails.append(f"overall 1σ coverage {o1*100:.1f}% outside "
                     f"{S2_OVERALL_LO*100:.0f}–{S2_OVERALL_HI*100:.0f}%")
    for t in ENTRY_TIMES_VAL:
        c1 = per_t[t]["vixd_corrected"]["cov_1sig"]
        if c1 is None or not (S2_TIME_LO <= c1 <= S2_TIME_HI):
            fails.append(f"entry time {t}: 1σ {c1*100:.1f}% outside "
                         f"{S2_TIME_LO*100:.0f}–{S2_TIME_HI*100:.0f}%")
    for r_ in REGIMES:
        c = per_r[r_]["vixd_corrected"]
        if c["n"] == 0:
            continue
        c1 = c["cov_1sig"]
        if not (S2_REGIME_LO <= c1 <= S2_REGIME_HI):
            fails.append(f"regime {r_}: 1σ {c1*100:.1f}% outside "
                         f"{S2_REGIME_LO*100:.0f}–{S2_REGIME_HI*100:.0f}%")

    tail_corr = overall["vixd_corrected"]["tail_3sig"]
    tail_raw  = overall["vixd_raw"]["tail_3sig"]
    print("\n  " + "!" * 74)
    print("  TAIL COVERAGE — model under-covers extremes; short-premium "
          "stop-outs live here.")
    print("  " + "!" * 74)
    print(f"    >3σ share after correction : {tail_corr*100:.2f}%   "
          f"(before {tail_raw*100:.2f}%, target {TAIL3_T*100:.1f}%)")
    print(f"    Scaling EM down thins the tail — this number is EXPECTED to worsen.")

    passed = len(fails) == 0
    print("\n  STAGE-2 RESULT: " + ("PASS" if passed else "FAIL"))
    if not passed:
        print("  Failing rows:")
        for f in fails:
            print(f"    ✗ {f}")

    res = {
        "window": {"start": te["date"].min(), "end": te["date"].max(),
                   "n_obs": int(len(te)), "n_days": int(te["date"].nunique())},
        "overall": overall, "per_entry_time": per_t, "per_regime": per_r,
        "pass": passed, "failing_rows": fails,
        "tail_3sig_corrected": tail_corr, "tail_3sig_raw": tail_raw,
    }
    return passed, res


# ── Stage 3 ───────────────────────────────────────────────────────────────────

def stage3(spx, vix, k_fn) -> dict:
    print("\n" + "=" * 78)
    print("STAGE 3 — RECALIBRATE OPTION MODEL   sigma_base(t) = (VIXD_t/100) x k(t)")
    print("=" * 78)

    prev = json.load(open(OUT_CAL)) if OUT_CAL.exists() else None

    k_fn.n_clamp_lo = k_fn.n_clamp_hi = 0
    params, model_form, rep = run_calibration(spx, vix, k_fn=k_fn)
    if k_fn.n_clamp_lo or k_fn.n_clamp_hi:
        print(f"      k(T) clamped for {k_fn.n_clamp_lo} print(s) before 09:35 "
              f"and {k_fn.n_clamp_hi} print(s) after 15:30.")

    p, gate = rep["params"], rep["gate_median_abs_pct_err"]

    print(f"\n  {'':<26} {'BEFORE (raw VIXD)':>20} {'AFTER (k-corrected)':>21}")
    print("  " + "-" * 70)
    if prev:
        pp = prev["params"]
        print(f"  {'a0':<26} {pp['a0']:>20.4f} {p['a0']:>21.4f}")
        print(f"  {'b':<26} {pp['b']:>20.4f} {p['b']:>21.4f}")
        print(f"  {'c':<26} {pp['c']:>20.4f} {p['c']:>21.4f}")
        print(f"  {'overall MAE ($)':<26} {prev['overall_mae']:>20.4f} "
              f"{rep['overall_mae']:>21.4f}")
        print(f"  {'median |%err| (all)':<26} "
              f"{prev['overall_median_abs_pct_err']*100:>19.2f}% "
              f"{rep['overall_median_abs_pct_err']*100:>20.2f}%")
        print(f"  {'median |%err| (px>=$0.50)':<26} "
              f"{prev['gate_median_abs_pct_err']*100:>19.2f}% {gate*100:>20.2f}%")
    else:
        print(f"  (no previous report found at {OUT_CAL.name})")
        print(f"  a0={p['a0']:.4f}  b={p['b']:.4f}  c={p['c']:.4f}  gate={gate*100:.2f}%")

    print(f"\n  {'Expiry':<14} {'n':>5} {'n>=.50':>7} {'MAE':>8} "
          f"{'med|%|>=.50':>12} {'signed_med':>11}")
    print("  " + "-" * 62)
    for exp, d in rep["per_expiry"].items():
        g = d["median_abs_pct_err_gate"]
        flag = "  *** BIAS ***" if abs(d["signed_median_err"]) > S3_BIAS_LIMIT else ""
        print(f"  {exp:<14} {d['n']:>5} {d['n_gate']:>7} ${d['mae']:>7.2f} "
              f"{(g*100 if g else float('nan')):>11.1f}% "
              f"${d['signed_median_err']:>9.3f}{flag}")

    print(f"\n  {'Time band':<14} {'n':>5} {'n>=.50':>7} {'MAE':>8} "
          f"{'med|%|>=.50':>12} {'signed_med':>11}")
    print("  " + "-" * 62)
    for band, d in rep["per_time_band"].items():
        g = d["median_abs_pct_err_gate"]
        sm = d["signed_median_err"]
        flag = "  *** BIAS ***" if sm is not None and abs(sm) > S3_BIAS_LIMIT else ""
        print(f"  {band:<14} {d['n']:>5} {d['n_gate']:>7} ${d['mae']:>7.2f} "
              f"{(g*100 if g else float('nan')):>11.1f}% "
              f"${sm:>9.3f}{flag}")

    # ── Verdict (pre-committed) ──
    bad_days  = [e for e, d in rep["per_expiry"].items()
                 if abs(d["signed_median_err"]) > S3_BIAS_LIMIT]
    bad_bands = [b for b, d in rep["per_time_band"].items()
                 if d["signed_median_err"] is not None
                 and abs(d["signed_median_err"]) > S3_BIAS_LIMIT]
    n_bad = len(bad_days) + len(bad_bands)

    if gate <= S3_GATE_PASS and n_bad == 0:
        verdict = "PASS"
    elif gate <= S3_GATE_PARTIAL and n_bad <= 1:
        verdict = "PARTIAL"
    else:
        verdict = "FAIL"

    a0_before = prev["params"]["a0"] if prev else None
    a0_toward_1 = (a0_before is not None
                   and abs(p["a0"] - 1.0) < abs(a0_before - 1.0))

    print("\n" + "=" * 78)
    print(f"STAGE-3 VERDICT: {verdict}")
    print("=" * 78)
    print(f"  median |%err| (px>=$0.50) = {gate*100:.2f}%   "
          f"(PASS<={S3_GATE_PASS*100:.0f}%, PARTIAL<={S3_GATE_PARTIAL*100:.0f}%)")
    print(f"  rows over ${S3_BIAS_LIMIT:.2f} bias limit: {n_bad}"
          + (f"  -> days {bad_days}  bands {bad_bands}" if n_bad else ""))
    if a0_before is not None:
        print(f"  a0 {a0_before:.4f} -> {p['a0']:.4f}   "
              f"{'MOVED TOWARD 1.0 (as predicted)' if a0_toward_1 else 'did NOT move toward 1.0'}")

    if verdict == "PARTIAL":
        print("\n  improved, not clean — offending row(s): "
              + ", ".join(bad_days + bad_bands))
    elif verdict == "FAIL":
        print("\n  Conversion fixed; residual option-print error is not a level or")
        print("  time-of-day effect. Single-index model cannot resolve it.")
        print("  Road A stops here.")

    rep["verdict"] = {
        "result": verdict, "gate_median_abs_pct_err": gate,
        "offending_days": bad_days, "offending_bands": bad_bands,
        "a0_before": a0_before, "a0_after": p["a0"],
        "a0_moved_toward_1": bool(a0_toward_1),
    }
    rep["k_clamp_counts"] = {"before_0935": k_fn.n_clamp_lo,
                             "after_1530": k_fn.n_clamp_hi}
    if prev:
        shutil.copy(OUT_CAL, PREV_CAL)
        print(f"\n  previous report saved -> {PREV_CAL.relative_to(REPO)}")
    with open(OUT_CAL, "w") as fh:
        json.dump(rep, fh, indent=2)
    print(f"  -> {OUT_CAL.relative_to(REPO)}")
    return rep


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 78)
    print("VIXD INTRADAY CONVERSION FACTOR k(T) + OPTION RECALIBRATION")
    print("k(T) fitted once on the fit window and FROZEN. No backtest.")
    print("=" * 78)

    print("\nLoading VIXD 1-min …")
    vixd, _ = load_vixd_1min()
    print("Loading SPX 1-min …")
    spx = load_all_bars("spx")
    print("Loading VIX 1-min …")
    vix = load_all_bars("vix")

    print("\nBuilding observations …")
    obs, meta = build_observations(spx, vix, vixd)
    print(f"  {meta['n_observations']:,} obs over {meta['n_trading_days']:,} days "
          f"({meta['window_start'][:10]} -> {meta['window_end'][:10]})")

    pts, k_fn = stage1(obs)
    s2_pass, s2 = stage2(obs, k_fn)

    payload = {
        "stage": "k_of_T_fit_and_verify",
        "k_divisor": K_DIVISOR,
        "smooth_form": "natural_cubic_spline",
        "clamp_policy": "k held at endpoint value outside 09:35–15:30",
        "fit_window": {"start": FIT_START, "end": FIT_END},
        "points": pts,
        "spline_knots_minute_of_day": [p["minute_of_day"] for p in pts],
        "spline_values": [p["k"] for p in pts],
        "test_window_stats": s2,
    }
    with open(OUT_K, "w") as fh:
        json.dump(payload, fh, indent=2)
    print(f"\n→ {OUT_K.relative_to(REPO)}")

    print("\n" + "-" * 78)
    print("LOOK-AHEAD AUDIT")
    print("-" * 78)
    print("  k(T)      : fitted ONLY on 2023-04-24…2025-05-31, frozen before Stage 2.")
    print("              Stage-2 and Stage-3 data never influence k.")
    print("  k(t) use  : depends only on the CLOCK (minute of day) — no market data.")
    print("  VIXD_t    : bar at or <=5 min before t              -> knowable at t")
    print("  S, VIX_t  : bar at or <=5 min before t              -> knowable at t")
    print("  S_close   : OUTCOME only — never an input to EM.")
    print("  Stage 3   : calibration days (Dec 2025 – May 2026) all fall AFTER the")
    print("              k fit window, so k is out-of-sample there too.")
    print("  -> No input depends on information unavailable at t.")

    if not s2_pass:
        print("\n" + "=" * 78)
        print("STOP — Stage 2 failed. Not proceeding to Stage 3.")
        print("k(T) is NOT refitted on the test window (pre-committed constraint).")
        print("=" * 78)
        sys.exit(1)

    stage3(spx, vix, k_fn)


if __name__ == "__main__":
    main()
