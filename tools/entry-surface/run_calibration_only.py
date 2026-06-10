"""Run calibration only — does not execute the backtest."""
import json, time
from pathlib import Path
from engine import load_all_bars, run_calibration, TOOLS, REPO

t0 = time.time()
print("Loading SPX…"); spx = load_all_bars("spx")
print("Loading VIX…"); vix = load_all_bars("vix")
print(f"SPX {len(spx):,} bars, VIX {len(vix):,} bars")
print("Running calibration…")
params, model_form, report = run_calibration(spx, vix)
p = report["params"]
med = report["overall_median_abs_pct_err"]
print(f"\nmodel={model_form}  a0={p['a0']:.4f}  a1={p['a1']:.4f}  b={p['b']:.4f}  c={p['c']:.4f}")
print(f"MAE=${report['overall_mae']:.4f}   median|%err|={med*100:.2f}%  ({time.time()-t0:.1f}s)")
if report["warning_unreliable"]:
    print("*** WARNING: median |%err| > 35% ***")
else:
    print("PASS: median |%err| <= 35%")
print("\nPer-expiry:")
for exp, d in report["per_expiry"].items():
    print(f"  {exp}: n={d['n']}  MAE=${d['mae']:.4f}  median|%err|={d['median_abs_pct_err']*100:.1f}%")
print("\nPer |m| band:")
for band, d in report["per_m_band"].items():
    sr = d.get("mean_signed_residual")
    sr_s = f"  bias=${sr:.4f}" if sr is not None else ""
    print(f"  {band}: n={d['n']}  MAE=${d['mae']:.4f}  median|%err|={d['median_abs_pct_err']*100:.1f}%{sr_s}")
print(f"\nSystematic curvature detected: {report['systematic_curvature_detected']}")
print(f"Per-band bias (m²):   {report['per_band_bias_m2']}")
print(f"Per-band bias (|m|):  {report['per_band_bias_abs_m']}")
print(f"MAE m²={report['mae_m2']:.4f}   MAE |m|={report['mae_abs_m']}")
out = TOOLS / "calibration_report.json"
with open(out, "w") as f:
    json.dump(report, f, indent=2)
print(f"\n→ {out.relative_to(REPO)}")
