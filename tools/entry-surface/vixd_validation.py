"""
VIXD full-window validation — does VIXD's LEVEL correctly size same-day SPX moves?

VALIDATION STAGE ONLY.  No option pricing, no refit of a0/b/c, no backtest.

For every trading day and entry time T:
    S        = SPX close of the bar at (or within 5 min before) T
    T_rem    = minutes from T to that day's close / (252 x 390)      [years]
    EM_x     = S x (x_T / 100) x sqrt(T_rem)      for x in {VIXD, VIX}
    Realized = |S_close - S|
    z_x      = Realized / EM_x

A well-calibrated input behaves like |N(0,1)|:
    median z  ~ 0.67      1-sigma coverage ~ 68%
    2-sigma coverage ~ 95%    share beyond 3-sigma ~ 0.3%

Signed calibration error = ln(median z / 0.67):
    > 0  =>  the input UNDER-states moves
    < 0  =>  the input OVER-states moves
"""

import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from engine import (
    DATA, TOOLS, REPO,
    US_PER_MIN, _ts_to_us, _parse_barchart_csv, load_all_bars,
)

# ── Config ────────────────────────────────────────────────────────────────────
VIXD_1MIN_DIR = DATA / "VIXD- 1min"
OUT_JSON      = TOOLS / "vixd_validation.json"

# Entry grid — starts 09:35 (VIXD 1-min bars begin 09:31, so 09:30 has no print)
ENTRY_TIMES_VAL = [
    "09:35", "10:00", "10:30", "11:00", "11:30", "12:00",
    "12:30", "13:00", "13:30", "14:00", "14:30", "15:00", "15:30",
]

Z_TARGET   = 0.67      # median of |N(0,1)| (exact 0.6745; 0.67 per pre-committed rule)
COV1_T     = 0.68
COV2_T     = 0.95
TAIL3_T    = 0.003
STALE_MIN  = 5         # a bar older than this (minutes) does not count as "at T"
ANNUAL_MIN = 252 * 390

# Pre-committed verdict thresholds (§4)
V_COV1_LO, V_COV1_HI = 0.62, 0.74
V_MEDZ_LO, V_MEDZ_HI = 0.50, 0.90

MONTHS = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11,
    "DECEMBER": 12,
}
_FNAME_RE = re.compile(r"download-([A-Z]+)-(\d{4})\.csv$", re.IGNORECASE)


def _f(x, nd=6):
    """JSON-safe float."""
    if x is None:
        return None
    x = float(x)
    return None if not math.isfinite(x) else round(x, nd)


# ── VIXD ingestion ────────────────────────────────────────────────────────────

def load_vixd_1min() -> tuple[pd.DataFrame, list]:
    """Load the 41 monthly VIXD files, ordered by parsed month NAME (not filename)."""
    if not VIXD_1MIN_DIR.is_dir():
        raise RuntimeError(f"HARD ERROR: VIXD directory missing: {VIXD_1MIN_DIR}")

    files = list(VIXD_1MIN_DIR.glob("*.csv"))
    if not files:
        raise RuntimeError(f"HARD ERROR: No CSVs in {VIXD_1MIN_DIR}")

    keyed = []
    for p in files:
        mt = _FNAME_RE.search(p.name)
        if not mt:
            raise RuntimeError(
                f"HARD ERROR: Cannot parse month/year from VIXD filename: {p.name}"
            )
        mname, yr = mt.group(1).upper(), int(mt.group(2))
        if mname not in MONTHS:
            raise RuntimeError(f"HARD ERROR: Unknown month name '{mname}' in {p.name}")
        keyed.append((yr, MONTHS[mname], p))

    keyed.sort(key=lambda t: (t[0], t[1]))        # chronological, NOT alphabetical

    frames = [_parse_barchart_csv(p) for _, _, p in keyed]
    df = pd.concat(frames)
    n_raw = len(df)
    df = df[~df.index.duplicated(keep="first")].sort_index()
    n_dedup = len(df)

    ordered = [(y, m) for y, m, _ in keyed]
    print(f"  VIXD files: {len(keyed)}  "
          f"({ordered[0][0]}-{ordered[0][1]:02d} … {ordered[-1][0]}-{ordered[-1][1]:02d})")
    print(f"  VIXD bars : {n_raw:,} raw → {n_dedup:,} after dedup "
          f"({n_raw - n_dedup:,} duplicate timestamps dropped)")
    return df, ordered


def vixd_coverage_table(vixd: pd.DataFrame, ordered: list) -> list:
    """Per-month trading days + RTH bar counts. Hard-errors on an empty month."""
    rth = vixd.between_time("09:30", "16:00")
    per = {}
    for (y, m), grp in rth.groupby([rth.index.year, rth.index.month]):
        per[(y, m)] = (len(np.unique(grp.index.normalize())), len(grp))

    rows, empty = [], []
    for (y, m) in ordered:
        nd, nb = per.get((y, m), (0, 0))
        rows.append({"month": f"{y}-{m:02d}", "n_days": nd, "n_bars_rth": nb})
        # April 2023 is the index launch month (first day 2023-04-24) — exempt.
        if nb == 0 and not (y == 2023 and m == 4):
            empty.append(f"{y}-{m:02d}")

    if empty:
        raise RuntimeError(
            "HARD ERROR: VIXD months with zero RTH rows (expected data): "
            + ", ".join(empty)
        )
    return rows


# ── Observation build ─────────────────────────────────────────────────────────

def build_observations(spx: pd.DataFrame, vix: pd.DataFrame,
                       vixd: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """One row per (trading day, entry time) with z for both inputs."""
    start = max(spx.index.min(), vix.index.min(), vixd.index.min())
    end   = min(spx.index.max(), vix.index.max(), vixd.index.max())

    def _win(df):
        return df[(df.index >= start) & (df.index <= end)].between_time("09:30", "16:00")

    spx_r, vix_r, vixd_r = _win(spx), _win(vix), _win(vixd)

    spx_us, spx_cl   = spx_r.index.asi8, spx_r["Close"].to_numpy(float)
    vix_us, vix_cl   = vix_r.index.asi8, vix_r["Close"].to_numpy(float)
    vixd_us, vixd_cl = vixd_r.index.asi8, vixd_r["Close"].to_numpy(float)

    # Per-day position of the last RTH SPX bar => that day's close.
    # On early-close days this is naturally the actual last bar (~13:00).
    day_key  = spx_r.index.normalize()
    pos      = pd.Series(np.arange(len(spx_r)), index=day_key)
    last_pos = pos.groupby(level=0).last()

    stale_us = STALE_MIN * US_PER_MIN
    recs = []
    skips = {"no_spx_bar": 0, "no_vixd_bar": 0, "no_vix_bar": 0,
             "t_at_or_after_close": 0, "bad_em": 0}

    for day, cpos in last_pos.items():
        close_us, S_close = int(spx_us[cpos]), float(spx_cl[cpos])

        for tstr in ENTRY_TIMES_VAL:
            hh, mm = int(tstr[:2]), int(tstr[3:])
            T_us = _ts_to_us(day + pd.Timedelta(hours=hh, minutes=mm))

            if close_us <= T_us:
                skips["t_at_or_after_close"] += 1
                continue

            i = np.searchsorted(spx_us, T_us, side="right") - 1
            if i < 0 or T_us - spx_us[i] > stale_us:
                skips["no_spx_bar"] += 1
                continue
            S = float(spx_cl[i])

            jd = np.searchsorted(vixd_us, T_us, side="right") - 1
            if jd < 0 or T_us - vixd_us[jd] > stale_us:
                skips["no_vixd_bar"] += 1
                continue
            v_d = float(vixd_cl[jd])

            jv = np.searchsorted(vix_us, T_us, side="right") - 1
            if jv < 0 or T_us - vix_us[jv] > stale_us:
                skips["no_vix_bar"] += 1
                continue
            v_v = float(vix_cl[jv])

            T_rem = ((close_us - T_us) / US_PER_MIN) / ANNUAL_MIN
            sq    = math.sqrt(T_rem)
            em_d  = S * (v_d / 100.0) * sq
            em_v  = S * (v_v / 100.0) * sq
            if not (em_d > 0 and em_v > 0):
                skips["bad_em"] += 1
                continue

            realized = abs(S_close - S)
            regime = "<20" if v_v < 20 else ("20-30" if v_v <= 30 else ">30")

            recs.append({
                "date": day.strftime("%Y-%m-%d"),
                "month": day.strftime("%Y-%m"),
                "entry_time": tstr,
                "S": S, "S_close": S_close, "realized": realized,
                "vixd": v_d, "vix": v_v,
                "em_vixd": em_d, "em_vix": em_v,
                "z_vixd": realized / em_d, "z_vix": realized / em_v,
                "regime": regime,
            })

    obs = pd.DataFrame(recs)
    meta = {
        "window_start": str(start), "window_end": str(end),
        "n_trading_days": int(len(last_pos)),
        "n_observations": int(len(obs)),
        "skips": skips,
    }
    return obs, meta


# ── Summaries ─────────────────────────────────────────────────────────────────

def summarise(z) -> dict:
    z = np.asarray(z, dtype=float)
    z = z[np.isfinite(z)]
    if len(z) == 0:
        return {"n": 0, "median_z": None, "cov_1sig": None,
                "cov_2sig": None, "tail_3sig": None, "signed_cal_err": None}
    med = float(np.median(z))
    return {
        "n": int(len(z)),
        "median_z": _f(med),
        "cov_1sig": _f((z <= 1).mean()),
        "cov_2sig": _f((z <= 2).mean()),
        "tail_3sig": _f((z > 3).mean()),
        "signed_cal_err": _f(math.log(med / Z_TARGET)) if med > 0 else None,
    }


def summarise_by(obs: pd.DataFrame, col: str, zcol: str, order=None) -> dict:
    out = {}
    keys = order if order is not None else sorted(obs[col].unique())
    for k in keys:
        sub = obs[obs[col] == k]
        out[str(k)] = summarise(sub[zcol].to_numpy())
    return out


def head_to_head(obs: pd.DataFrame) -> dict:
    """Per entry_time x regime cell: which input's coverage sits closer to 68%/95%?

    Win metric (stated, pre-committed): combined distance
        |cov_1sig - 0.68| + |cov_2sig - 0.95|
    Also reported: 1-sigma-only and 2-sigma-only tallies, and an n>=30 subset.
    """
    regimes = ["<20", "20-30", ">30"]
    cells = []
    for t in ENTRY_TIMES_VAL:
        for r in regimes:
            sub = obs[(obs["entry_time"] == t) & (obs["regime"] == r)]
            if len(sub) == 0:
                continue
            sd = summarise(sub["z_vixd"].to_numpy())
            sv = summarise(sub["z_vix"].to_numpy())
            d_d = abs(sd["cov_1sig"] - COV1_T) + abs(sd["cov_2sig"] - COV2_T)
            d_v = abs(sv["cov_1sig"] - COV1_T) + abs(sv["cov_2sig"] - COV2_T)
            cells.append({
                "entry_time": t, "regime": r, "n": int(len(sub)),
                "vixd_cov1": sd["cov_1sig"], "vix_cov1": sv["cov_1sig"],
                "vixd_cov2": sd["cov_2sig"], "vix_cov2": sv["cov_2sig"],
                "vixd_dist": _f(d_d), "vix_dist": _f(d_v),
                "winner": "vixd" if d_d < d_v else ("vix" if d_v < d_d else "tie"),
                "winner_1sig": (
                    "vixd" if abs(sd["cov_1sig"] - COV1_T) < abs(sv["cov_1sig"] - COV1_T)
                    else "vix" if abs(sv["cov_1sig"] - COV1_T) < abs(sd["cov_1sig"] - COV1_T)
                    else "tie"),
                "winner_2sig": (
                    "vixd" if abs(sd["cov_2sig"] - COV2_T) < abs(sv["cov_2sig"] - COV2_T)
                    else "vix" if abs(sv["cov_2sig"] - COV2_T) < abs(sd["cov_2sig"] - COV2_T)
                    else "tie"),
            })

    def _tally(rows, key):
        return {
            "vixd": sum(1 for c in rows if c[key] == "vixd"),
            "vix":  sum(1 for c in rows if c[key] == "vix"),
            "tie":  sum(1 for c in rows if c[key] == "tie"),
            "n_cells": len(rows),
        }

    thick = [c for c in cells if c["n"] >= 30]
    return {
        "cells": cells,
        "tally_combined": _tally(cells, "winner"),
        "tally_1sig": _tally(cells, "winner_1sig"),
        "tally_2sig": _tally(cells, "winner_2sig"),
        "tally_combined_n30": _tally(thick, "winner"),
    }


def build_verdict(vd: dict, h2h: dict) -> dict:
    """Pre-committed §4 rule. No softening."""
    fails, detail = [], {}

    c1 = vd["overall"]["cov_1sig"]
    ok1 = c1 is not None and V_COV1_LO <= c1 <= V_COV1_HI
    detail["cond1_overall_cov1_in_62_74"] = {
        "pass": bool(ok1), "value": c1, "range": [V_COV1_LO, V_COV1_HI]}
    if not ok1:
        fails.append("cond1: overall 1-sigma coverage outside 62–74%")

    bad_reg = {k: v["median_z"] for k, v in vd["per_regime"].items()
               if v["median_z"] is None or not (V_MEDZ_LO <= v["median_z"] <= V_MEDZ_HI)}
    ok2 = len(bad_reg) == 0
    detail["cond2_regime_medz_in_0.50_0.90"] = {
        "pass": bool(ok2), "offending_rows": bad_reg, "range": [V_MEDZ_LO, V_MEDZ_HI]}
    if not ok2:
        fails.append("cond2: VIX-regime median z outside 0.50–0.90")

    tc = h2h["tally_combined"]
    ok3 = tc["vixd"] > tc["n_cells"] / 2
    detail["cond3_vixd_beats_vix_majority"] = {
        "pass": bool(ok3), "vixd_wins": tc["vixd"], "vix_wins": tc["vix"],
        "ties": tc["tie"], "n_cells": tc["n_cells"]}
    if not ok3:
        fails.append("cond3: VIXD does not beat VIX in a majority of "
                     "entry-time x regime cells")

    return {"pass": len(fails) == 0, "failed_conditions": fails, "detail": detail}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 78)
    print("VIXD FULL-WINDOW VALIDATION — expected move vs realized move")
    print("VALIDATION ONLY: no option pricing, no refit, no backtest.")
    print("=" * 78)

    print("\nLoading VIXD 1-min …")
    vixd, ordered = load_vixd_1min()
    cov_rows = vixd_coverage_table(vixd, ordered)

    print("\nLoading SPX 1-min …")
    spx = load_all_bars("spx")
    print("Loading VIX 1-min …")
    vix = load_all_bars("vix")
    print(f"  SPX {len(spx):,} bars   VIX {len(vix):,} bars")

    # ── Coverage limitation: VIXD extends past the SPX/VIX archives ──
    limitation = None
    if vixd.index.max() > min(spx.index.max(), vix.index.max()):
        limitation = (
            f"VIXD data runs to {vixd.index.max().date()}, but the SPX/VIX archives "
            f"end {min(spx.index.max(), vix.index.max()).date()}. Months beyond that "
            f"CANNOT be tested — the test needs all three series. Testable window is "
            f"the intersection below, NOT the full VIXD history."
        )

    print("\nVIXD monthly coverage (RTH 09:30–16:00):")
    print(f"  {'month':<9} {'days':>5} {'bars':>8}     {'month':<9} {'days':>5} {'bars':>8}")
    half = (len(cov_rows) + 1) // 2
    for a in range(half):
        L = cov_rows[a]
        R = cov_rows[a + half] if a + half < len(cov_rows) else None
        ls = f"  {L['month']:<9} {L['n_days']:>5} {L['n_bars_rth']:>8}"
        rs = (f"     {R['month']:<9} {R['n_days']:>5} {R['n_bars_rth']:>8}") if R else ""
        print(ls + rs)
    print(f"  First VIXD day: {vixd.index.min().date()} (index launch — expected, not a gap)")

    print("\nBuilding observations …")
    obs, meta = build_observations(spx, vix, vixd)
    if limitation:
        print("\n" + "!" * 78)
        print("LIMITATION — DATA COVERAGE")
        print("!" * 78)
        for line in limitation.split(". "):
            if line.strip():
                print("  " + line.strip().rstrip(".") + ".")
        print("!" * 78)
    meta["limitation"] = limitation

    print(f"\n  Window     : {meta['window_start']} → {meta['window_end']}")
    print(f"  Days       : {meta['n_trading_days']:,}")
    print(f"  Obs (day×T): {meta['n_observations']:,}")
    print(f"  Skipped    : {meta['skips']}")

    if len(obs) == 0:
        raise RuntimeError("HARD ERROR: no observations built — check data overlap.")

    regimes = ["<20", "20-30", ">30"]
    months  = sorted(obs["month"].unique())
    res = {}
    for tag, zc in (("vixd", "z_vixd"), ("vix", "z_vix")):
        res[tag] = {
            "overall":        summarise(obs[zc].to_numpy()),
            "per_entry_time": summarise_by(obs, "entry_time", zc, ENTRY_TIMES_VAL),
            "per_regime":     summarise_by(obs, "regime", zc, regimes),
            "per_month":      summarise_by(obs, "month", zc, months),
        }

    h2h = head_to_head(obs)
    verdict = build_verdict(res["vixd"], h2h)

    # 10 worst VIXD days by z
    worst = obs.nlargest(10, "z_vixd")[
        ["date", "entry_time", "vixd", "em_vixd", "realized", "z_vixd"]]
    worst_rows = [{"date": r.date, "entry_time": r.entry_time,
                   "vixd": _f(r.vixd, 2), "em_vixd": _f(r.em_vixd, 2),
                   "realized": _f(r.realized, 2), "z_vixd": _f(r.z_vixd, 3)}
                  for r in worst.itertuples()]

    # ── Console report ──
    def _pct(x):
        return "  n/a " if x is None else f"{x*100:5.1f}%"

    print("\n" + "=" * 78)
    print("OVERALL")
    print("=" * 78)
    print(f"  {'input':<6} {'n':>7} {'med z':>7} {'1σ cov':>8} {'2σ cov':>8} "
          f"{'>3σ':>7} {'ln(z/0.67)':>11}")
    for tag in ("vixd", "vix"):
        o = res[tag]["overall"]
        print(f"  {tag:<6} {o['n']:>7,} {o['median_z']:>7.3f} "
              f"{_pct(o['cov_1sig']):>8} {_pct(o['cov_2sig']):>8} "
              f"{_pct(o['tail_3sig']):>7} {o['signed_cal_err']:>11.4f}")
    print(f"  targets{'':<0} {'':>7} {Z_TARGET:>7.3f} {COV1_T*100:>7.1f}% "
          f"{COV2_T*100:>7.1f}% {TAIL3_T*100:>6.1f}% {0.0:>11.4f}")

    print("\n" + "-" * 78)
    print("PER ENTRY TIME")
    print("-" * 78)
    print(f"  {'T':<7} {'n':>6} | {'VIXD medz':>9} {'1σ':>7} {'2σ':>7} "
          f"| {'VIX medz':>9} {'1σ':>7} {'2σ':>7}")
    for t in ENTRY_TIMES_VAL:
        d, v = res["vixd"]["per_entry_time"][t], res["vix"]["per_entry_time"][t]
        if d["n"] == 0:
            continue
        print(f"  {t:<7} {d['n']:>6,} | {d['median_z']:>9.3f} "
              f"{_pct(d['cov_1sig'])} {_pct(d['cov_2sig'])} "
              f"| {v['median_z']:>9.3f} {_pct(v['cov_1sig'])} {_pct(v['cov_2sig'])}")

    print("\n" + "-" * 78)
    print("PER VIX REGIME (at T)")
    print("-" * 78)
    print(f"  {'regime':<8} {'n':>6} | {'VIXD medz':>9} {'1σ':>7} {'2σ':>7} {'signed':>8} "
          f"| {'VIX medz':>9} {'1σ':>7} {'signed':>8}")
    for r in regimes:
        d, v = res["vixd"]["per_regime"][r], res["vix"]["per_regime"][r]
        if d["n"] == 0:
            print(f"  {r:<8} {0:>6} | (no observations)")
            continue
        print(f"  {r:<8} {d['n']:>6,} | {d['median_z']:>9.3f} "
              f"{_pct(d['cov_1sig'])} {_pct(d['cov_2sig'])} {d['signed_cal_err']:>8.3f} "
              f"| {v['median_z']:>9.3f} {_pct(v['cov_1sig'])} {v['signed_cal_err']:>8.3f}")

    print("\n" + "-" * 78)
    print("MEDIAN z BY CALENDAR MONTH (drift check)")
    print("-" * 78)
    print(f"  {'month':<9} {'n':>5} {'VIXD':>7} {'VIX':>7}    "
          f"{'month':<9} {'n':>5} {'VIXD':>7} {'VIX':>7}")
    half_m = (len(months) + 1) // 2
    for a in range(half_m):
        def _row(idx):
            if idx >= len(months):
                return ""
            mo = months[idx]
            d, v = res["vixd"]["per_month"][mo], res["vix"]["per_month"][mo]
            return (f"  {mo:<9} {d['n']:>5} {d['median_z']:>7.3f} {v['median_z']:>7.3f}")
        print(_row(a) + "   " + _row(a + half_m))

    print("\n" + "-" * 78)
    print("VIXD ROWS FURTHEST FROM 68% 1σ COVERAGE")
    print("-" * 78)
    far_t = sorted(
        [(t, res["vixd"]["per_entry_time"][t]) for t in ENTRY_TIMES_VAL
         if res["vixd"]["per_entry_time"][t]["n"] > 0],
        key=lambda kv: -abs(kv[1]["cov_1sig"] - COV1_T))[:5]
    for t, d in far_t:
        print(f"  entry {t}   1σ cov {_pct(d['cov_1sig'])}  "
              f"(Δ {(d['cov_1sig']-COV1_T)*100:+.1f} pts)  med z {d['median_z']:.3f}")
    far_r = sorted(
        [(r, res["vixd"]["per_regime"][r]) for r in regimes
         if res["vixd"]["per_regime"][r]["n"] > 0],
        key=lambda kv: -abs(kv[1]["cov_1sig"] - COV1_T))
    for r, d in far_r:
        print(f"  regime {r:<7} 1σ cov {_pct(d['cov_1sig'])}  "
              f"(Δ {(d['cov_1sig']-COV1_T)*100:+.1f} pts)  med z {d['median_z']:.3f}")

    print("\n" + "-" * 78)
    print("HEAD-TO-HEAD (entry_time × regime cells; win = coverage closer to 68%/95%)")
    print("-" * 78)
    for lbl, key in (("combined 1σ+2σ", "tally_combined"),
                     ("1σ only", "tally_1sig"),
                     ("2σ only", "tally_2sig"),
                     ("combined, n≥30", "tally_combined_n30")):
        t = h2h[key]
        print(f"  {lbl:<16} VIXD {t['vixd']:>3}  |  VIX {t['vix']:>3}  |  "
              f"tie {t['tie']:>3}   (of {t['n_cells']} cells)")

    print("\n" + "-" * 78)
    print("TAIL CHECK — 10 worst VIXD observations (target: 0.3% beyond 3σ)")
    print("-" * 78)
    o_d, o_v = res["vixd"]["overall"], res["vix"]["overall"]
    print(f"  share > 3×EM :  VIXD {o_d['tail_3sig']*100:.2f}%   "
          f"VIX {o_v['tail_3sig']*100:.2f}%   (target {TAIL3_T*100:.1f}%)")
    print(f"  {'date':<12} {'T':<7} {'VIXD':>7} {'EM':>9} {'realized':>10} {'z':>7}")
    for w in worst_rows:
        print(f"  {w['date']:<12} {w['entry_time']:<7} {w['vixd']:>7.2f} "
              f"{w['em_vixd']:>9.2f} {w['realized']:>10.2f} {w['z_vixd']:>7.2f}")

    print("\n" + "-" * 78)
    print("LOOK-AHEAD AUDIT")
    print("-" * 78)
    print("  S        : SPX bar at or ≤5 min before T          → knowable at T")
    print("  VIXD_T   : VIXD bar at or ≤5 min before T         → knowable at T")
    print("  VIX_T    : VIX  bar at or ≤5 min before T         → knowable at T")
    print("  regime   : derived from VIX_T only                → knowable at T")
    print("  T_rem    : uses the day's scheduled close time.  Early closes are")
    print("             published in advance, so this is knowable at T.")
    print("  S_close  : OUTCOME being predicted — never an input to EM.")
    print("  → No input depends on information unavailable at T.")

    print("\n" + "=" * 78)
    print("VERDICT (pre-committed §4)")
    print("=" * 78)
    for name, d in verdict["detail"].items():
        print(f"  [{'PASS' if d['pass'] else 'FAIL'}] {name}")
        if not d["pass"]:
            for k, v in d.items():
                if k != "pass":
                    print(f"         {k}: {v}")
    print()
    if verdict["pass"]:
        print("  VIXD PASSES — level sizes same-day SPX moves within pre-committed bounds.")
    else:
        print("  VIXD FAILS.  Failed conditions:")
        for f in verdict["failed_conditions"]:
            print(f"    ✗ {f}")
    print("=" * 78)

    payload = {
        "stage": "validation_only",
        "meta": meta,
        "targets": {"median_z": Z_TARGET, "cov_1sig": COV1_T,
                    "cov_2sig": COV2_T, "tail_3sig": TAIL3_T},
        "entry_times": ENTRY_TIMES_VAL,
        "vixd_monthly_coverage": cov_rows,
        "vixd": res["vixd"],
        "vix": res["vix"],
        "head_to_head": h2h,
        "worst_days_vixd": worst_rows,
        "verdict": verdict,
    }
    with open(OUT_JSON, "w") as fh:
        json.dump(payload, fh, indent=2)
    print(f"\n→ {OUT_JSON.relative_to(REPO)}")


if __name__ == "__main__":
    main()
