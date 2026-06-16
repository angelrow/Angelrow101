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
gate = report["gate_median_abs_pct_err"]
print(f"\n{'='*60}")
print(f"Model:   IV = (VIXD/100) × max(a0 + b·m + c·|m|, 0.05)  [σ_base = VIXD per print]")
print(f"Params:  a0={p['a0']:.4f}   b={p['b']:.4f}   c={p['c']:.4f}")
print(f"{'='*60}")
print(f"Overall  MAE=${report['overall_mae']:.4f}   "
      f"median|%err|={report['overall_median_abs_pct_err']*100:.1f}%")
print(f"Gate     n={report['n_gate_prints']}  "
      f"median|%err| (px≥$0.50) = {gate*100:.2f}%  "
      f"{'PASS ✓' if gate <= 0.35 else 'FAIL ✗'}")

print(f"\n{'─'*60}")
print(f"{'Expiry':<14} {'n':>5} {'n≥0.50':>7} {'MAE':>7} "
      f"{'med|%|':>8} {'med|%|≥.50':>11} {'signed_med':>11} {'bias?':>6}")
print(f"{'─'*60}")
for exp, d in report["per_expiry"].items():
    flag = " *** BIAS ***" if d["bias_flag"] else ""
    g = d["median_abs_pct_err_gate"]
    print(f"{exp:<14} {d['n']:>5} {d['n_gate']:>7} "
          f"${d['mae']:>6.2f} "
          f"{d['median_abs_pct_err']*100:>7.1f}% "
          f"{(g*100 if g else float('nan')):>10.1f}% "
          f"${d['signed_median_err']:>9.3f}{flag}")

print(f"\n{'─'*60}")
print(f"{'Band':<14} {'n':>5} {'MAE':>7} "
      f"{'med|%|':>8} {'med|%|≥.50':>11} {'signed_med':>11}")
print(f"{'─'*60}")
for band, d in report["per_m_band"].items():
    g = d["median_abs_pct_err_gate"]
    print(f"{band:<14} {d['n']:>5} "
          f"${d['mae']:>6.2f} "
          f"{(d['median_abs_pct_err']*100 if d['median_abs_pct_err'] else float('nan')):>7.1f}% "
          f"{(g*100 if g else float('nan')):>10.1f}% "
          f"${d['signed_median_err']:>9.3f}")

print(f"\n{'─'*60}")
print(f"{'Time band':<14} {'n':>5} {'n≥0.50':>7} {'MAE':>7} "
      f"{'med|%|':>8} {'med|%|≥.50':>11} {'signed_med':>11}")
print(f"{'─'*60}")
for band, d in report["per_time_band"].items():
    g = d["median_abs_pct_err_gate"]
    print(f"{band:<14} {d['n']:>5} {d['n_gate']:>7} "
          f"${d['mae']:>6.2f} "
          f"{(d['median_abs_pct_err']*100 if d['median_abs_pct_err'] else float('nan')):>7.1f}% "
          f"{(g*100 if g else float('nan')):>10.1f}% "
          f"${d['signed_median_err']:>9.3f}")

if report["biased_days"]:
    print(f"\n*** BIASED DAYS (|signed median| > $1): {report['biased_days']} ***")
else:
    print("\nNo biased days detected (all |signed median| ≤ $1).")

out = TOOLS / "calibration_report.json"
with open(out, "w") as f:
    json.dump(report, f, indent=2)
print(f"\n→ {out.relative_to(REPO)}")
print(f"Total: {time.time()-t0:.1f}s")
