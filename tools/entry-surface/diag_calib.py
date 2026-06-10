"""
Calibration diagnostic — run standalone.
Prints 15 sample rows and checks VIX units, timestamp matching, T_rem, params, m sign.
"""
import json, math, sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import norm

REPO      = Path(__file__).resolve().parents[2]
DATA      = REPO / "trading_data"
TOOLS     = Path(__file__).resolve().parent
CALIB_CSV = TOOLS / "calibration" / "calib_dataset.csv"

RISK_FREE   = 0.045
DIV_YIELD   = 0.013
US_PER_MIN  = 60_000_000   # μs per minute

def _ts_to_us(ts):
    return ts.value // 1000

def _bsm_put(S, K, T, r, q, sigma):
    if T <= 0:
        return max(K - S, 0.0)
    sq = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5*sigma**2)*T) / sq
    d2 = d1 - sq
    return K*math.exp(-r*T)*norm.cdf(-d2) - S*math.exp(-q*T)*norm.cdf(-d1)

def _parse_barchart_csv(path):
    raw = pd.read_csv(path, on_bad_lines="warn", low_memory=False)
    raw = raw[raw["Time"].str.match(r"^\d{4}-\d{2}-\d{2}", na=False)].copy()
    raw["Time"] = raw["Time"].str.strip('"')
    raw["Time"] = pd.to_datetime(raw["Time"], format="%Y-%m-%d %H:%M")
    raw = raw.dropna(subset=["Time"])
    raw = raw.rename(columns={"Latest": "Close"})
    for col in ("Open","High","Low","Close"):
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=["Close"])
    raw = raw.sort_values("Time").reset_index(drop=True)
    raw["Time"] = raw["Time"].dt.tz_localize(
        "America/New_York", ambiguous="infer", nonexistent="shift_forward"
    )
    return raw.set_index("Time")[["Open","High","Low","Close"]]

print("Loading bars …", flush=True)
with open(DATA / "manifest.json") as f:
    manifest = json.load(f)

# Load only the months that contain our four expiry dates
# expiries: 2025-12-23, 2026-03-03, 2026-03-13, 2026-05-22
# + the months they sit in (for the prints during those exiry days)
NEEDED_MONTHS = {
    "2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05",
}

def _month_key(path_str):
    # path like "...SPX - JANUARY 2016 ... .csv"
    # or pull YYYY-MM from the filename
    p = Path(path_str)
    stem = p.stem
    # extract 4-digit year and month name from stem
    import re
    m = re.search(r'(\w+)[-\s]+(\d{4})', stem)
    if m:
        month_str, year = m.group(1), m.group(2)
        months = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE",
                  "JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"]
        try:
            mo = months.index(month_str.upper()) + 1
            return f"{year}-{mo:02d}"
        except ValueError:
            pass
    return None

spx_frames, vix_frames = [], []
for rel in manifest["spx"]:
    mk = _month_key(rel)
    if mk in NEEDED_MONTHS:
        spx_frames.append(_parse_barchart_csv(DATA / rel))

for rel in manifest["vix"]:
    mk = _month_key(rel)
    if mk in NEEDED_MONTHS:
        vix_frames.append(_parse_barchart_csv(DATA / rel))

spx = pd.concat(spx_frames).sort_index()
vix = pd.concat(vix_frames).sort_index()
spx = spx[~spx.index.duplicated(keep="first")]
vix = vix[~vix.index.duplicated(keep="first")]
print(f"  SPX bars: {len(spx):,}   VIX bars: {len(vix):,}")

# ── Load calib ────────────────────────────────────────────────────────────────
calib = pd.read_csv(CALIB_CSV)
calib["ts"] = pd.to_datetime(calib["timestamp_et"]).dt.tz_localize(
    "America/New_York", ambiguous="infer", nonexistent="shift_forward"
)
calib["expiry_date"] = pd.to_datetime(calib["expiry"]).dt.date
print(f"  Calib rows: {len(calib):,}   Expiry dates: {sorted(calib['expiry_date'].unique())}")

# ── Bar matching (same logic as engine.py) ────────────────────────────────────
spx_us = spx.index.asi8
spx_cl = spx["Close"].values
vix_us = vix.index.asi8
vix_cl = vix["Close"].values
ts_us  = pd.DatetimeIndex(calib["ts"]).asi8

spx_idx = np.searchsorted(spx_us, ts_us, side="right") - 1
vix_idx = np.searchsorted(vix_us, ts_us, side="right") - 1
valid   = (spx_idx >= 0) & (vix_idx >= 0)

calib_v  = calib[valid].copy().reset_index(drop=True)
spx_idx_v = spx_idx[valid]
vix_idx_v = vix_idx[valid]
ts_us_v   = ts_us[valid]

S_arr   = spx_cl[spx_idx_v]
vix_arr = vix_cl[vix_idx_v]
K_arr   = calib_v["strike"].values.astype(float)
px_arr  = calib_v["price_last"].values.astype(float)

# Expiry settlement at 16:00 ET
expiry_us = np.array([
    _ts_to_us(
        pd.Timestamp(str(r["expiry_date"]))
          .tz_localize("America/New_York")
          .replace(hour=16, minute=0, second=0)
    )
    for _, r in calib_v.iterrows()
], dtype=np.int64)

mins_rem  = (expiry_us - ts_us_v) / US_PER_MIN
T_rem_arr = np.maximum(mins_rem, 0.0) / (252 * 390)

sigma_arr  = vix_arr / 100.0          # VIX/100 → annualised sigma_base
vix_sc_arr = vix_arr / 20.0           # VIX/20  → vix_scalar

denom = sigma_arr * np.sqrt(np.maximum(T_rem_arr, 1e-12))
m_arr = np.log(K_arr / S_arr) / np.maximum(denom, 1e-12)

# ── Fitted params from last engine run ───────────────────────────────────────
PARAMS = (1.3119, -0.7138, -0.8120, -0.2863)
a0, a1, b, c = PARAMS

def model_iv(sigma_base, vix_sc, m):
    raw = a0 + a1*vix_sc + b*m + c*m**2
    return sigma_base * max(raw, 0.05)

def model_px(S, K, T, sigma_base, vix_sc, m):
    iv = model_iv(sigma_base, vix_sc, m)
    return _bsm_put(S, K, T, RISK_FREE, DIV_YIELD, iv)

# ── Invert market IV for each print ──────────────────────────────────────────
iv_market = np.full(len(px_arr), np.nan)
for i in range(len(px_arr)):
    S, K, T, px = S_arr[i], K_arr[i], T_rem_arr[i], px_arr[i]
    if T <= 0 or px <= 0:
        continue
    try:
        lo = _bsm_put(S, K, T, RISK_FREE, DIV_YIELD, 0.001)
        hi = _bsm_put(S, K, T, RISK_FREE, DIV_YIELD, 20.0)
        if hi < px:
            iv_market[i] = 20.0
        elif lo > px:
            iv_market[i] = 0.001
        else:
            iv_market[i] = brentq(
                lambda sig: _bsm_put(S, K, T, RISK_FREE, DIV_YIELD, sig) - px,
                0.001, 20.0, xtol=1e-6, maxiter=100
            )
    except Exception:
        pass

iv_model_arr = np.array([
    model_iv(sigma_arr[i], vix_sc_arr[i], m_arr[i])
    for i in range(len(S_arr))
])
px_model_arr = np.array([
    model_px(S_arr[i], K_arr[i], T_rem_arr[i], sigma_arr[i], vix_sc_arr[i], m_arr[i])
    for i in range(len(S_arr))
])

# ── Select 15 diagnostic rows: ~4 per expiry, spread across moneyness ─────────
print("\n" + "="*140)
print("DIAGNOSTIC TABLE — 15 sample calibration prints")
print("="*140)
hdr = (
    f"{'#':>3}  {'expiry':<12} {'timestamp_et':<20} {'K':>7} {'S_spx':>8} {'VIX':>6} "
    f"{'T_rem_min':>10} {'m':>7} {'σ_base':>8} {'IV_mkt':>8} {'IV_mdl':>8} "
    f"{'px_act':>8} {'px_mdl':>8} {'abs_err':>8}"
)
print(hdr)
print("-"*140)

expiry_dates = sorted(calib_v["expiry_date"].unique())
selected = []
for exp in expiry_dates:
    mask = calib_v["expiry_date"].values == exp
    idxs = np.where(mask)[0]
    # sort by |m| to get spread across moneyness: pick ~4 rows
    order = np.argsort(m_arr[idxs])   # ascending m (most OTM first)
    step  = max(1, len(order) // 4)
    picks = [idxs[order[j]] for j in range(0, len(order), step)][:4]
    selected.extend(picks)

# Fill to 15
remaining = [i for i in range(len(S_arr)) if i not in selected]
selected = (selected + remaining)[:15]
selected.sort()

for n, i in enumerate(selected, 1):
    ts_str = calib_v["ts"].iloc[i].strftime("%Y-%m-%d %H:%M")
    exp    = str(calib_v["expiry_date"].iloc[i])
    K      = K_arr[i]
    S      = S_arr[i]
    vix_v  = vix_arr[i]
    mins   = mins_rem[i]
    m      = m_arr[i]
    sb     = sigma_arr[i]
    iv_mk  = iv_market[i]
    iv_md  = iv_model_arr[i]
    px_a   = px_arr[i]
    px_m   = px_model_arr[i]
    aerr   = abs(px_m - px_a)

    iv_mk_s = f"{iv_mk*100:.2f}%" if np.isfinite(iv_mk) else "  n/a "
    print(
        f"{n:>3}  {exp:<12} {ts_str:<20} {K:>7.0f} {S:>8.2f} {vix_v:>6.2f} "
        f"{mins:>10.1f} {m:>7.3f} {sb:>8.4f} {iv_mk_s:>8} {iv_md*100:>7.2f}% "
        f"{px_a:>8.4f} {px_m:>8.4f} {aerr:>8.4f}"
    )

# ── Check 1: VIX units ─────────────────────────────────────────────────────────
print("\n" + "="*80)
print("CHECK 1 — VIX UNITS")
print("="*80)
sample_vix = vix_arr[:20]
sample_sb  = sigma_arr[:20]
print(f"  VIX values (first 20): {np.round(sample_vix, 2)}")
print(f"  sigma_base = VIX/100  (first 20): {np.round(sample_sb, 4)}")
print(f"  vix_scalar = VIX/20   (first 20): {np.round(vix_sc_arr[:20], 4)}")
print(f"  → sigma_base range: [{sample_sb.min():.4f}, {sample_sb.max():.4f}]  (should be ≈0.08–0.50 for sane VIX)")
print(f"  → Looks {'OK' if 0.05 < sample_sb.mean() < 1.0 else 'WRONG'}: mean sigma_base = {sample_sb.mean():.4f}")

# ── Check 2: Timestamp matching ───────────────────────────────────────────────
print("\n" + "="*80)
print("CHECK 2 — TIMESTAMP MATCHING")
print("="*80)
for i in selected[:5]:
    ts   = calib_v["ts"].iloc[i]
    ts_u = ts_us_v[i]
    si   = spx_idx_v[i]
    vi   = vix_idx_v[i]
    spx_bar_ts = spx.index[si]
    vix_bar_ts = vix.index[vi]
    lag_spx = (ts_u - spx_us[si]) / US_PER_MIN
    lag_vix = (ts_u - vix_us[vi]) / US_PER_MIN
    print(f"  print ts={ts.strftime('%Y-%m-%d %H:%M ET')}  "
          f"→  SPX bar={spx_bar_ts.strftime('%Y-%m-%d %H:%M')} (lag {lag_spx:.1f}min)  "
          f"VIX bar={vix_bar_ts.strftime('%Y-%m-%d %H:%M')} (lag {lag_vix:.1f}min)")

# ── Check 3: T_rem ────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("CHECK 3 — T_REM (trading-time convention, 252×390 min/yr)")
print("="*80)
for exp in expiry_dates:
    mask = calib_v["expiry_date"].values == exp
    mn   = mins_rem[mask]
    tr   = T_rem_arr[mask]
    print(f"  expiry {exp}:  mins_rem range [{mn.min():.1f}, {mn.max():.1f}]  "
          f"→  T_rem range [{tr.min():.6f}, {tr.max():.6f}]  "
          f"(annualised:  {tr.min()*252*390:.1f}–{tr.max()*252*390:.1f} min)")
print(f"  1 trading day = 390 min → T_rem = 1/252 = {1/252:.6f}  ✓" if abs(390/(252*390) - 1/252) < 1e-9 else "  MISMATCH")

# ── Check 4: Fitted params ────────────────────────────────────────────────────
print("\n" + "="*80)
print("CHECK 4 — FITTED PARAMS AND MODEL BEHAVIOUR")
print("="*80)
print(f"  a0={a0:.4f}  a1={a1:.4f}  b={b:.4f}  c={c:.4f}")
for vix_test in [14, 20, 30]:
    vs = vix_test / 20
    sb = vix_test / 100
    for m_test in [0.0, -1.0, -2.0]:
        raw  = a0 + a1*vs + b*m_test + c*m_test**2
        fac  = max(raw, 0.05)
        iv_t = sb * fac
        px_t = _bsm_put(5600, 5600*math.exp(m_test*sb*math.sqrt(1/252)), 1/252, RISK_FREE, DIV_YIELD, iv_t)
        print(f"    VIX={vix_test:2d} m={m_test:5.1f}  raw_factor={raw:.3f}  clamped={fac:.3f}  IV={iv_t*100:.2f}%  px≈${px_t:.3f}")

print(f"\n  ATM (m=0) factor: a0 + a1*(VIX/20) = {a0:.4f} + {a1:.4f}*(VIX/20)")
print(f"    → at VIX=14: {a0 + a1*0.7:.4f}  at VIX=20: {a0+a1*1.0:.4f}  at VIX=30: {a0+a1*1.5:.4f}")

# ── Check 5: m sign/scale for OTM puts ────────────────────────────────────────
print("\n" + "="*80)
print("CHECK 5 — m SIGN & SCALE FOR OTM PUTS  (m = ln(K/S)/[σ_base√T])")
print("="*80)
otm  = m_arr < -0.5
atm  = np.abs(m_arr) <= 0.5
itm  = m_arr > 0.5
print(f"  OTM puts (m < -0.5):   {otm.sum():4d} rows  mean m={m_arr[otm].mean():.3f}  median px=${np.median(px_arr[otm]):.4f}")
print(f"  Near-ATM (|m|≤0.5):   {atm.sum():4d} rows  mean m={m_arr[atm].mean():.3f}  median px=${np.median(px_arr[atm]):.4f}")
print(f"  ITM puts (m >  0.5):   {itm.sum():4d} rows  mean m={m_arr[itm].mean():.3f}  median px=${np.median(px_arr[itm]):.4f}")
print(f"\n  For a put, OTM = strike BELOW spot → ln(K/S) < 0 → m < 0  ✓" if otm.sum() > atm.sum() else
      "  WARNING: fewer OTM rows than expected — check moneyness convention")
print(f"\n  m range overall: [{m_arr.min():.3f}, {m_arr.max():.3f}]")
print(f"  Market IV range: [{np.nanmin(iv_market)*100:.2f}%, {np.nanmax(iv_market)*100:.2f}%]   "
      f"(finite: {np.isfinite(iv_market).sum()}/{len(iv_market)})")
print(f"  sigma_base range: [{sigma_arr.min()*100:.2f}%, {sigma_arr.max()*100:.2f}%]")

# Summary: is iv_market / sigma_base consistent with a reasonable (a0+...)  factor?
ratio = iv_market[np.isfinite(iv_market)] / sigma_arr[np.isfinite(iv_market)]
print(f"\n  iv_market/sigma_base ratio (= target 'factor'):")
print(f"    mean={ratio.mean():.3f}  median={np.median(ratio):.3f}  "
      f"p10={np.percentile(ratio,10):.3f}  p90={np.percentile(ratio,90):.3f}")
print(f"    → model must hit factor ≈ {np.median(ratio):.3f} at ATM to match market prices")

print("\n" + "="*80)
print("DONE")
