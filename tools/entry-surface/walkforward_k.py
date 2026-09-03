"""
Final Road A attempt: walk-forward k(T) + VRP discriminating test.

Stage 0  VRP discriminating test (always runs, never blocks anything).
         Is the falling k (0.739 -> 0.598, 2023-2026) a rising same-day
         variance risk premium, or variance migrating into the overnight?
         Recompute z against full 24-hour realised moves and see whether the
         drift survives.

Stage 1  Walk-forward trailing k(T): for each month M from 2024-05, fit on
         ONLY the 12 months ending the day before M starts.  That curve is the
         only one applied inside M.  No observation ever sees a curve fitted
         on its own month or later.

Stage 2  Verify on 2025-06-01 -> last common date, against the SAME bands as
         the frozen-curve attempt.  Bands are not widened.

Stage 3  Option recalibration, only if Stage 2 passes.

NOTATION: k(T) is the intraday conversion factor.  K (capital) is a strike in
the engine.  They are unrelated.
"""

import json
import math
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from engine import TOOLS, REPO, load_all_bars, run_calibration
from vixd_validation import (
    ENTRY_TIMES_VAL, load_vixd_1min, build_observations, summarise,
)
from fit_k_of_T import (
    make_k_fn, _mod, _f, _pct, K_DIVISOR, Z_TARGET, COV1_T, COV2_T, TAIL3_T,
    K_MIN_SANE, K_MAX_SANE, REGIMES,
    S2_OVERALL_LO, S2_OVERALL_HI, S2_TIME_LO, S2_TIME_HI,
    S2_REGIME_LO, S2_REGIME_HI,
    S3_GATE_PASS, S3_GATE_PARTIAL, S3_BIAS_LIMIT,
    OUT_CAL, PREV_CAL,
)

WF_START_MONTH = "2024-05"          # first month with a full trailing year
TRAILING_MONTHS = 12
TEST_START = "2025-06-01"

OUT_VRP = TOOLS / "vrp_drift_test.json"
OUT_WF  = TOOLS / "k_of_T_walkforward.json"
OUT_KFR = TOOLS / "k_of_T.json"      # frozen curve from ceb52f9, for comparison

TRADING_DAYS_YR = 252
SAMPLE_MONTHS = ["2024-06", "2025-01", "2025-09", "2026-04"]


# ── Curve fitting (shared) ────────────────────────────────────────────────────

def fit_curve(sub: pd.DataFrame, label: str) -> tuple:
    """13 medians -> k points -> natural cubic spline. Hard-errors on insanity."""
    pts = []
    for t in ENTRY_TIMES_VAL:
        z = sub[sub["entry_time"] == t]["z_vixd"].to_numpy()
        z = z[np.isfinite(z)]
        if len(z) == 0:
            raise RuntimeError(
                f"HARD ERROR: curve '{label}' has no observations at entry time {t}."
            )
        med = float(np.median(z))
        pts.append({"entry_time": t, "minute_of_day": _mod(t), "n": int(len(z)),
                    "median_z": _f(med), "k": _f(med / K_DIVISOR)})

    bad = [(p["entry_time"], p["k"]) for p in pts
           if not (K_MIN_SANE <= p["k"] <= K_MAX_SANE)]
    if bad:
        raise RuntimeError(
            f"HARD ERROR: curve '{label}' has k(T) outside "
            f"[{K_MIN_SANE}, {K_MAX_SANE}] at: "
            + ", ".join(f"{t} k={k:.4f}" for t, k in bad)
        )

    x  = np.array([p["minute_of_day"] for p in pts], dtype=float)
    ks = np.array([p["k"] for p in pts], dtype=float)
    kf = make_k_fn(x, ks)

    grid = np.arange(_mod("09:30"), _mod("16:00") + 1, dtype=float)
    kg = kf(grid)
    kf.n_clamp_lo = kf.n_clamp_hi = 0
    if kg.min() < K_MIN_SANE or kg.max() > K_MAX_SANE:
        raise RuntimeError(
            f"HARD ERROR: smoothed curve '{label}' leaves "
            f"[{K_MIN_SANE}, {K_MAX_SANE}] (min {kg.min():.4f}, max {kg.max():.4f})."
        )
    return pts, kf


# ── Stage 0 ───────────────────────────────────────────────────────────────────

def stage0(obs: pd.DataFrame, spx: pd.DataFrame) -> dict:
    print("\n" + "=" * 78)
    print("STAGE 0 — VRP DISCRIMINATING TEST  (informational; blocks nothing)")
    print("=" * 78)
    print("  intraday realised : |S_close_today - S_T|")
    print("  24h realised      : |S_T_next_trading_day - S_T|  (includes the gap)")
    print("  both scored against the SAME VIXD_T reading; 24h uses T = 1/252.")

    # Trading-day sequence from SPX itself (not from obs) so a day missing at
    # one entry time cannot silently shift "next trading day" by two.
    rth = spx.between_time("09:30", "16:00")
    days = pd.Index(sorted(set(rth.index.normalize())))
    nxt = {d.strftime("%Y-%m-%d"): days[i + 1].strftime("%Y-%m-%d")
           for i, d in enumerate(days[:-1])}

    s_map = {(r.date, r.entry_time): r.S for r in obs.itertuples()}

    rows = []
    n_no_next = 0
    for r in obs.itertuples():
        nd = nxt.get(r.date)
        if nd is None:
            n_no_next += 1
            continue
        s_next = s_map.get((nd, r.entry_time))
        if s_next is None:
            n_no_next += 1
            continue
        em24 = r.S * (r.vixd / 100.0) * math.sqrt(1.0 / TRADING_DAYS_YR)
        if not em24 > 0:
            continue
        rows.append({"date": r.date, "year": r.date[:4], "entry_time": r.entry_time,
                     "z_intraday": r.z_vixd,
                     "z_24h": abs(s_next - r.S) / em24})
    d24 = pd.DataFrame(rows)
    print(f"\n  paired observations: {len(d24):,}  "
          f"({n_no_next:,} dropped — no next-day bar at the same clock time)")

    years = sorted(d24["year"].unique())
    per_year = {}
    print(f"\n  {'year':<6} {'n':>7} {'k_intraday':>11} {'k_24h':>9} "
          f"{'24h 1σ':>8} {'24h 2σ':>8}")
    print("  " + "-" * 54)
    for y in years:
        sub = d24[d24["year"] == y]
        ki = float(np.median(sub["z_intraday"])) / K_DIVISOR
        s24 = summarise(sub["z_24h"].to_numpy())
        k24 = s24["median_z"] / K_DIVISOR
        per_year[y] = {"n": int(len(sub)), "k_intraday": _f(ki), "k_24h": _f(k24),
                       "median_z_24h": s24["median_z"],
                       "cov_1sig_24h": s24["cov_1sig"],
                       "cov_2sig_24h": s24["cov_2sig"],
                       "tail_3sig_24h": s24["tail_3sig"]}
        print(f"  {y:<6} {len(sub):>7,} {ki:>11.4f} {k24:>9.4f} "
              f"{_pct(s24['cov_1sig']):>8} {_pct(s24['cov_2sig']):>8}")

    ov_i = float(np.median(d24["z_intraday"])) / K_DIVISOR
    ov24 = summarise(d24["z_24h"].to_numpy())
    print(f"  {'ALL':<6} {len(d24):>7,} {ov_i:>11.4f} "
          f"{ov24['median_z']/K_DIVISOR:>9.4f} "
          f"{_pct(ov24['cov_1sig']):>8} {_pct(ov24['cov_2sig']):>8}")

    # ── Pre-committed reading ──
    k24s = [per_year[y]["k_24h"] for y in years]
    kis  = [per_year[y]["k_intraday"] for y in years]
    drops24 = sum(1 for a, b in zip(k24s, k24s[1:]) if b < a)
    n_trans = len(k24s) - 1
    tot_dec24 = (k24s[0] - k24s[-1]) / k24s[0] if k24s[0] else 0.0
    spread24  = (max(k24s) - min(k24s)) / float(np.mean(k24s))
    intraday_declines = kis[-1] < kis[0]

    print(f"\n  k_24h transitions falling: {drops24}/{n_trans}   "
          f"total relative decline: {tot_dec24*100:+.1f}%")
    print(f"  k_24h relative spread (max-min)/mean: {spread24*100:.1f}%   "
          f"k_intraday declines overall: {intraday_declines}")

    if drops24 >= 2 and tot_dec24 >= 0.10:
        reading = "VRP GROWTH — drift survives in 24h realised; the same-day premium is widening."
    elif spread24 <= 0.05 and intraday_declines:
        reading = "OVERNIGHT MIGRATION — drift is a measurement artefact of intraday-only realised."
    else:
        reading = "MIXED"
    print(f"\n  >>> {reading}")
    if reading == "MIXED":
        print(f"      k_intraday: " + "  ".join(f"{y}={v:.4f}" for y, v in zip(years, kis)))
        print(f"      k_24h     : " + "  ".join(f"{y}={v:.4f}" for y, v in zip(years, k24s)))

    # ── Overnight share of daily variance ──
    # log returns: gap = ln(next_open / close), full = ln(next_close / close)
    first = rth.groupby(rth.index.normalize()).first()
    last  = rth.groupby(rth.index.normalize()).last()
    dd = pd.DataFrame({"open": first["Open"], "close": last["Close"]}).sort_index()
    dd["next_open"]  = dd["open"].shift(-1)
    dd["next_close"] = dd["close"].shift(-1)
    dd = dd.dropna()
    dd["gap"]  = np.log(dd["next_open"]  / dd["close"])
    dd["full"] = np.log(dd["next_close"] / dd["close"])
    dd["year"] = dd.index.year.astype(str)

    print(f"\n  OVERNIGHT SHARE OF DAILY VARIANCE  (log returns; "
          f"gap = close->next open, full = close->next close)")
    print(f"  {'year':<6} {'n':>5} {'var(gap)':>12} {'var(full)':>12} {'share':>8}")
    print("  " + "-" * 46)
    on_share = {}
    for y in sorted(dd["year"].unique()):
        s = dd[dd["year"] == y]
        vg, vf = float(s["gap"].var()), float(s["full"].var())
        sh = vg / vf if vf > 0 else None
        on_share[y] = {"n": int(len(s)), "var_gap": _f(vg, 10),
                       "var_full": _f(vf, 10), "overnight_share": _f(sh)}
        print(f"  {y:<6} {len(s):>5} {vg:>12.3e} {vf:>12.3e} {sh*100:>7.1f}%")

    payload = {"stage": "vrp_discriminating_test", "n_paired": int(len(d24)),
               "per_year": per_year,
               "overall": {"k_intraday": _f(ov_i),
                           "k_24h": _f(ov24["median_z"] / K_DIVISOR), **ov24},
               "reading": reading,
               "test_definitions": {
                   "vrp_growth": "k_24h falls in >=2 of 3 transitions AND total relative decline >=10%",
                   "overnight_migration": "k_24h (max-min)/mean <=5% AND k_intraday declines",
               },
               "overnight_variance_share": on_share}
    with open(OUT_VRP, "w") as fh:
        json.dump(payload, fh, indent=2)
    print(f"\n  → {OUT_VRP.relative_to(REPO)}")
    return payload


# ── Stage 1 ───────────────────────────────────────────────────────────────────

def stage1(obs: pd.DataFrame) -> tuple:
    print("\n" + "=" * 78)
    print("STAGE 1 — WALK-FORWARD TRAILING k(T)")
    print("=" * 78)
    print(f"  For month M: fit on the {TRAILING_MONTHS} months ending the day "
          f"before M starts.")
    print("  That curve is the ONLY one applied inside M. Causal by construction.")

    all_months = sorted(obs["month"].unique())
    wf_months  = [m for m in all_months if m >= WF_START_MONTH]
    warmup     = [m for m in all_months if m < WF_START_MONTH]
    print(f"\n  warm-up (excluded, no full trailing year): "
          f"{warmup[0]} … {warmup[-1]}  ({len(warmup)} months)")
    print(f"  walk-forward curves: {wf_months[0]} … {wf_months[-1]}  "
          f"({len(wf_months)} months)")

    curves, curve_pts = {}, {}
    for m in wf_months:
        m_start = pd.Timestamp(m + "-01")
        fit_lo  = (m_start - pd.DateOffset(months=TRAILING_MONTHS)).strftime("%Y-%m-%d")
        fit_hi  = (m_start - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        sub = obs[(obs["date"] >= fit_lo) & (obs["date"] <= fit_hi)]
        if len(sub) == 0:
            raise RuntimeError(
                f"HARD ERROR: month {m} has no data in trailing window "
                f"{fit_lo}..{fit_hi}."
            )
        pts, kf = fit_curve(sub, f"{m} (fit {fit_lo}..{fit_hi})")
        curves[m] = kf
        curve_pts[m] = {"fit_start": fit_lo, "fit_end": fit_hi,
                        "n_obs": int(len(sub)), "points": pts}

    print(f"\n  all {len(wf_months)} curves inside [{K_MIN_SANE}, {K_MAX_SANE}]  OK")

    shown = [m for m in SAMPLE_MONTHS if m in curve_pts]
    print(f"\n  SAMPLE TRAILING CURVES (drift-tracking visible)")
    hdr = "  " + f"{'T':<8}" + "".join(f"{m:>10}" for m in shown)
    print(hdr)
    print("  " + "-" * (8 + 10 * len(shown)))
    for i, t in enumerate(ENTRY_TIMES_VAL):
        line = f"  {t:<8}"
        for m in shown:
            line += f"{curve_pts[m]['points'][i]['k']:>10.4f}"
        print(line)
    line = f"  {'mean':<8}"
    for m in shown:
        line += f"{np.mean([p['k'] for p in curve_pts[m]['points']]):>10.4f}"
    print(line)

    with open(OUT_WF, "w") as fh:
        json.dump({"stage": "walk_forward_k_of_T",
                   "trailing_months": TRAILING_MONTHS,
                   "k_divisor": K_DIVISOR,
                   "smooth_form": "natural_cubic_spline",
                   "clamp_policy": "k held at endpoint value outside 09:35–15:30",
                   "warmup_months": warmup,
                   "curves": curve_pts}, fh, indent=2)
    print(f"\n  → {OUT_WF.relative_to(REPO)}")
    return curves, curve_pts, warmup


# ── Stage 2 ───────────────────────────────────────────────────────────────────

def stage2(obs: pd.DataFrame, curves: dict) -> tuple:
    print("\n" + "=" * 78)
    print("STAGE 2 — VERIFY WALK-FORWARD  (bands identical to the frozen attempt)")
    print("=" * 78)

    te = obs[obs["date"] >= TEST_START].copy()
    if len(te) == 0:
        raise RuntimeError("HARD ERROR: no observations in the test window.")

    missing = sorted(set(te["month"]) - set(curves))
    if missing:
        raise RuntimeError(
            f"HARD ERROR: test months without a walk-forward curve: {missing}"
        )

    mod = te["entry_time"].map(_mod).to_numpy(dtype=float)
    kw = np.empty(len(te), dtype=float)
    for m in sorted(set(te["month"])):
        msk = (te["month"] == m).to_numpy()
        kw[msk] = curves[m](mod[msk])
    te["k_wf"] = kw
    te["z_wf"] = te["z_vixd"] / te["k_wf"]

    # Frozen curve from ceb52f9, for a like-for-like column.
    if not OUT_KFR.exists():
        raise RuntimeError(f"HARD ERROR: frozen curve missing: {OUT_KFR}")
    fr = json.load(open(OUT_KFR))
    frozen = make_k_fn(np.array(fr["spline_knots_minute_of_day"], dtype=float),
                       np.array(fr["spline_values"], dtype=float))
    te["z_frozen"] = te["z_vixd"] / frozen(mod)

    print(f"  observations: {len(te):,}   days: {te['date'].nunique():,}   "
          f"({te['date'].min()} -> {te['date'].max()})")
    print(f"  months covered: {te['month'].min()} … {te['month'].max()}   "
          f"each using its own trailing curve.")

    def _quad(frame):
        return {"walkforward": summarise(frame["z_wf"].to_numpy()),
                "frozen":      summarise(frame["z_frozen"].to_numpy()),
                "raw":         summarise(frame["z_vixd"].to_numpy())}

    overall = _quad(te)
    per_t   = {t: _quad(te[te["entry_time"] == t]) for t in ENTRY_TIMES_VAL}
    per_r   = {r: _quad(te[te["regime"] == r]) for r in REGIMES}

    print("\n  OVERALL (test window)")
    print(f"  {'input':<20} {'n':>7} {'med z':>8} {'1σ cov':>8} {'2σ cov':>8} {'>3σ':>7}")
    for lbl, key in (("walk-forward k(T)", "walkforward"),
                     ("frozen k(T) [ceb52f9]", "frozen"),
                     ("uncorrected VIXD", "raw")):
        s = overall[key]
        print(f"  {lbl:<20} {s['n']:>7,} {s['median_z']:>8.3f} "
              f"{_pct(s['cov_1sig']):>8} {_pct(s['cov_2sig']):>8} {_pct(s['tail_3sig']):>7}")
    print(f"  {'target':<20} {'':>7} {Z_TARGET:>8.3f} {COV1_T*100:>7.1f}% "
          f"{COV2_T*100:>7.1f}% {TAIL3_T*100:>6.1f}%")

    print("\n  PER ENTRY TIME — 1σ coverage")
    print(f"  {'T':<8} {'n':>6} {'wf 1σ':>8} {'frozen 1σ':>10} {'raw 1σ':>8} "
          f"{'wf medz':>9} {'wf 2σ':>8}")
    for t in ENTRY_TIMES_VAL:
        w, f0, r0 = per_t[t]["walkforward"], per_t[t]["frozen"], per_t[t]["raw"]
        print(f"  {t:<8} {w['n']:>6,} {_pct(w['cov_1sig']):>8} "
              f"{_pct(f0['cov_1sig']):>10} {_pct(r0['cov_1sig']):>8} "
              f"{w['median_z']:>9.3f} {_pct(w['cov_2sig']):>8}")

    print("\n  PER VIX REGIME — 1σ coverage")
    print(f"  {'regime':<8} {'n':>6} {'wf 1σ':>8} {'frozen 1σ':>10} {'raw 1σ':>8} "
          f"{'wf medz':>9} {'wf 2σ':>8}")
    for r_ in REGIMES:
        w, f0, r0 = per_r[r_]["walkforward"], per_r[r_]["frozen"], per_r[r_]["raw"]
        if w["n"] == 0:
            print(f"  {r_:<8} {0:>6} (no observations)")
            continue
        print(f"  {r_:<8} {w['n']:>6,} {_pct(w['cov_1sig']):>8} "
              f"{_pct(f0['cov_1sig']):>10} {_pct(r0['cov_1sig']):>8} "
              f"{w['median_z']:>9.3f} {_pct(w['cov_2sig']):>8}")

    # ── Pass rule — identical bands, not widened ──
    fails = []
    o1 = overall["walkforward"]["cov_1sig"]
    if not (S2_OVERALL_LO <= o1 <= S2_OVERALL_HI):
        fails.append(f"overall 1σ coverage {o1*100:.1f}% outside "
                     f"{S2_OVERALL_LO*100:.0f}–{S2_OVERALL_HI*100:.0f}%")
    for t in ENTRY_TIMES_VAL:
        c1 = per_t[t]["walkforward"]["cov_1sig"]
        if c1 is None or not (S2_TIME_LO <= c1 <= S2_TIME_HI):
            fails.append(f"entry time {t}: 1σ {c1*100:.1f}% outside "
                         f"{S2_TIME_LO*100:.0f}–{S2_TIME_HI*100:.0f}%")
    for r_ in REGIMES:
        c = per_r[r_]["walkforward"]
        if c["n"] == 0:
            continue
        if not (S2_REGIME_LO <= c["cov_1sig"] <= S2_REGIME_HI):
            fails.append(f"regime {r_}: 1σ {c['cov_1sig']*100:.1f}% outside "
                         f"{S2_REGIME_LO*100:.0f}–{S2_REGIME_HI*100:.0f}%")

    tw, tf, tr = (overall["walkforward"]["tail_3sig"],
                  overall["frozen"]["tail_3sig"], overall["raw"]["tail_3sig"])
    print("\n  " + "!" * 74)
    print("  TAIL COVERAGE — model under-covers extremes; short-premium "
          "stop-outs live here.")
    print("  " + "!" * 74)
    print(f"    >3σ share:  walk-forward {tw*100:.2f}%   frozen {tf*100:.2f}%   "
          f"uncorrected {tr*100:.2f}%   (target {TAIL3_T*100:.1f}%)")

    passed = len(fails) == 0
    print("\n  STAGE-2 RESULT: " + ("PASS" if passed else "FAIL"))
    if not passed:
        print("  Failing rows:")
        for f in fails:
            print(f"    ✗ {f}")

    return passed, {"overall": overall, "per_entry_time": per_t,
                    "per_regime": per_r, "pass": passed, "failing_rows": fails}


# ── Stage 3 ───────────────────────────────────────────────────────────────────

def make_wf_k_fn(curves: dict):
    def k_fn(mod, ts=None):
        if ts is None:
            raise RuntimeError(
                "HARD ERROR: walk-forward k(T) requires timestamps to select "
                "the month's curve; k_fn was called without them."
            )
        mod = np.asarray(mod, dtype=float)
        months = pd.DatetimeIndex(ts).strftime("%Y-%m").to_numpy()
        out = np.empty(len(mod), dtype=float)
        for mo in np.unique(months):
            if mo not in curves:
                raise RuntimeError(
                    f"HARD ERROR: no walk-forward curve for calibration month {mo}."
                )
            msk = (months == mo)
            out[msk] = curves[mo](mod[msk])
        return out
    return k_fn


def stage3(spx, vix, curves) -> dict:
    print("\n" + "=" * 78)
    print("STAGE 3 — OPTION RECALIBRATION  σ_base(t) = (VIXD_t/100) × k_M(T(t))")
    print("=" * 78)

    prev = json.load(open(OUT_CAL)) if OUT_CAL.exists() else None
    params, model_form, rep = run_calibration(spx, vix, k_fn=make_wf_k_fn(curves))
    p, gate = rep["params"], rep["gate_median_abs_pct_err"]

    print(f"\n  {'':<26} {'BEFORE (raw VIXD)':>20} {'AFTER (walk-fwd k)':>21}")
    print("  " + "-" * 70)
    if prev:
        pp = prev["params"]
        for lbl, a, b in (("a0", pp["a0"], p["a0"]), ("b", pp["b"], p["b"]),
                          ("c", pp["c"], p["c"]),
                          ("overall MAE ($)", prev["overall_mae"], rep["overall_mae"])):
            print(f"  {lbl:<26} {a:>20.4f} {b:>21.4f}")
        print(f"  {'median |%err| (px>=$0.50)':<26} "
              f"{prev['gate_median_abs_pct_err']*100:>19.2f}% {gate*100:>20.2f}%")

    for title, block in (("Expiry", rep["per_expiry"]),
                         ("Time band", rep["per_time_band"])):
        print(f"\n  {title:<14} {'n':>5} {'n>=.50':>7} {'MAE':>8} "
              f"{'med|%|>=.50':>12} {'signed_med':>11}")
        print("  " + "-" * 62)
        for key, d in block.items():
            g, sm = d["median_abs_pct_err_gate"], d["signed_median_err"]
            flag = "  *** BIAS ***" if sm is not None and abs(sm) > S3_BIAS_LIMIT else ""
            print(f"  {key:<14} {d['n']:>5} {d['n_gate']:>7} ${d['mae']:>7.2f} "
                  f"{(g*100 if g else float('nan')):>11.1f}% ${sm:>9.3f}{flag}")

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
    a0_toward = (a0_before is not None and abs(p["a0"] - 1.0) < abs(a0_before - 1.0))

    print("\n" + "=" * 78)
    print(f"STAGE-3 VERDICT: {verdict}")
    print("=" * 78)
    print(f"  median |%err| (px>=$0.50) = {gate*100:.2f}%")
    print(f"  rows over ${S3_BIAS_LIMIT:.2f} bias limit: {n_bad}"
          + (f"  -> days {bad_days}  bands {bad_bands}" if n_bad else ""))
    if a0_before is not None:
        print(f"  a0 {a0_before:.4f} -> {p['a0']:.4f}   "
              f"{'moved toward 1.0' if a0_toward else 'did NOT move toward 1.0'}")
    if verdict == "FAIL":
        print("\n  Conversion fixed and drift-tracked; residual option-print error")
        print("  is structural. Single-index model cannot resolve it.")
        print("  Road A ends here.")

    rep["verdict"] = {"result": verdict, "offending_days": bad_days,
                      "offending_bands": bad_bands, "a0_before": a0_before,
                      "a0_after": p["a0"], "a0_moved_toward_1": bool(a0_toward)}
    if verdict in ("PASS", "PARTIAL"):
        if prev:
            shutil.copy(OUT_CAL, PREV_CAL)
        with open(OUT_CAL, "w") as fh:
            json.dump(rep, fh, indent=2)
        print(f"\n  → {OUT_CAL.relative_to(REPO)} (previous kept as {PREV_CAL.name})")
    else:
        print(f"\n  calibration_report.json NOT overwritten (verdict FAIL).")
    return rep


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 78)
    print("FINAL ROAD A ATTEMPT — walk-forward k(T) + VRP discriminating test")
    print("Stage-2 bands are NOT widened. This is the last attempt by prior agreement.")
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

    stage0(obs, spx)
    curves, curve_pts, warmup = stage1(obs)
    s2_pass, s2 = stage2(obs, curves)

    print("\n" + "-" * 78)
    print("LOOK-AHEAD AUDIT")
    print("-" * 78)
    print("  walk-forward : month M uses a curve fitted on the 12 months ENDING")
    print("                 the day before M starts. No observation is scored by a")
    print("                 curve that saw its own month or anything later.")
    print("  warm-up      : 2023-04…2024-04 excluded — no full trailing year.")
    print("  k(t) use     : depends only on the CLOCK and the month index.")
    print("  VIXD/VIX/S   : bar at or <=5 min before t          -> knowable at t")
    print("  S_close      : OUTCOME only — never an input to EM.")
    print("  Stage 0      : measurement only; nothing downstream is fitted on it.")
    print("                 S_T_next is a FUTURE observation used only as a target,")
    print("                 never as a model input.")
    print("  -> No input depends on information unavailable at t.")

    if not s2_pass:
        print("\n" + "=" * 78)
        print("ROAD A ENDS. Walk-forward k(T) does not calibrate out of sample. "
              "No further fits. Document and park.")
        print("=" * 78)
        sys.exit(1)

    stage3(spx, vix, curves)


if __name__ == "__main__":
    main()
