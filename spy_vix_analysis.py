import os, math, warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy.stats import chi2_contingency, fisher_exact
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT

warnings.filterwarnings('ignore')

# ── paths ──────────────────────────────────────────────────────────────────────
DATA_DIR   = '/home/user/Angelrow101/data'
OUT_DIR    = '/mnt/user-data/outputs'
os.makedirs(OUT_DIR, exist_ok=True)

SPY_CSV = os.path.join(DATA_DIR, 'spy_daily_historical-data-04-04-2026.csv')
VIX_CSV = os.path.join(DATA_DIR, 'vix_daily_historical-data-04-04-2026.csv')

# ── colours ────────────────────────────────────────────────────────────────────
C_DARK   = '#1a1a1a'
C_GREY   = '#f5f5f5'
C_GREEN  = '#00a86b'
C_RED    = '#cc0000'
C_WHITE  = '#ffffff'

# ══════════════════════════════════════════════════════════════════════════════
# 1. DATA LOADING & PREP
# ══════════════════════════════════════════════════════════════════════════════
def parse_date(s):
    for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    return pd.NaT

def load_csv(path):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['date'] = df['Time'].apply(parse_date)
    df = df.dropna(subset=['date'])
    df = df[['date', 'Latest']].rename(columns={'Latest': 'close'})
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df = df.dropna(subset=['close'])
    return df.sort_values('date').reset_index(drop=True)

print("Loading data…")
spy = load_csv(SPY_CSV)
vix = load_csv(VIX_CSV)

merged = pd.merge(spy, vix, on='date', suffixes=('_spy', '_vix'))
merged = merged.sort_values('date').reset_index(drop=True)

merged['spy_return'] = merged['close_spy'].pct_change() * 100
merged = merged.iloc[1:].reset_index(drop=True)   # drop first row (no return)

merged['breach_1pct_put']  = (merged['spy_return'] < -1.0).astype(int)
merged['breach_1pct_call'] = (merged['spy_return'] >  1.0).astype(int)
merged['breach_2pct_put']  = (merged['spy_return'] < -2.0).astype(int)

print(f"  {len(merged)} trading days after merge & drop")

# ── bucket assignment helper ───────────────────────────────────────────────────
def assign_bucket(vix_series, scheme):
    labels = pd.Series('', index=vix_series.index)
    for lo, hi, name in scheme:
        mask = (vix_series >= lo) & (vix_series < hi)
        labels[mask] = name
    return labels

NEG_INF, POS_INF = float('-inf'), float('inf')

SCHEME_A = [(NEG_INF,12,'<12'),(12,18,'12-18'),(18,25,'18-25'),(25,35,'25-35'),(35,POS_INF,'>35')]
SCHEME_B = [(NEG_INF,12,'<12'),(12,14,'12-14'),(14,16,'14-16'),(16,18,'16-18'),
            (18,21,'18-21'),(21,25,'21-25'),(25,30,'25-30'),(30,40,'30-40'),(40,POS_INF,'>40')]
SCHEME_C = [(NEG_INF,11,'<11'),(11,12,'11-12'),(12,13,'12-13'),(13,14,'13-14'),
            (14,15,'14-15'),(15,16,'15-16'),(16,17,'16-17'),(17,18,'17-18'),
            (18,20,'18-20'),(20,22,'20-22'),(22,25,'22-25'),(25,30,'25-30'),
            (30,40,'30-40'),(40,POS_INF,'>40')]

merged['vix_bucket_5'] = assign_bucket(merged['close_vix'], SCHEME_A)
merged['vix_bucket_9'] = assign_bucket(merged['close_vix'], SCHEME_B)
merged['vix_bucket_14']= assign_bucket(merged['close_vix'], SCHEME_C)
merged['vix_bucket_int']= merged['close_vix'].apply(math.floor).astype(str)

# ── VIX direction labels ───────────────────────────────────────────────────────
merged['vix_direction_1d']  = np.where(merged['close_vix'] > merged['close_vix'].shift(1),  'Rising', 'Falling')
merged['vix_direction_5d']  = np.where(merged['close_vix'] > merged['close_vix'].shift(5),  'Rising', 'Falling')
vix_ma10 = merged['close_vix'].rolling(10).mean()
merged['vix_direction_10dma']= np.where(merged['close_vix'] > vix_ma10, 'Rising', 'Falling')
merged.loc[merged['close_vix'].shift(1).isna(), 'vix_direction_1d']   = np.nan
merged.loc[merged['close_vix'].shift(5).isna(), 'vix_direction_5d']   = np.nan
merged.loc[vix_ma10.isna(), 'vix_direction_10dma'] = np.nan

# ══════════════════════════════════════════════════════════════════════════════
# 2. HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def wilson_ci(k, n, z=1.96):
    if n == 0: return (0.0, 0.0)
    p = k / n
    denom = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denom
    margin = z * math.sqrt(p*(1-p)/n + z**2/(4*n**2)) / denom
    return max(0.0, center - margin), min(1.0, center + margin)

def ci_width(k, n):
    lo, hi = wilson_ci(k, n)
    return hi - lo

def pct(x): return f"{x*100:.1f}%"

def bucket_stats(df, bucket_col, ordered_labels):
    rows = []
    for lbl in ordered_labels:
        sub = df[df[bucket_col] == lbl]
        n   = len(sub)
        k1p = sub['breach_1pct_put'].sum()
        k1c = sub['breach_1pct_call'].sum()
        k2p = sub['breach_2pct_put'].sum()
        r1p = k1p/n if n else 0
        r1c = k1c/n if n else 0
        r2p = k2p/n if n else 0
        w   = ci_width(k1p, n)
        rows.append([lbl, n, pct(r1p), pct(r1c), pct(r2p), f"{w*100:.1f}%"])
    return rows

def scheme_labels(scheme):
    return [name for _,_,name in scheme]

def int_bucket_labels(df):
    vals = sorted(df['vix_bucket_int'].unique(), key=lambda x: int(x))
    return vals

# ══════════════════════════════════════════════════════════════════════════════
# 3. ANALYSIS 1 — BUCKET TABLES
# ══════════════════════════════════════════════════════════════════════════════
print("Analysis 1: bucket tables…")
HDR_COLS = ['Bucket','Days','1% Put Breach','1% Call Breach','2% Put Breach','95% CI Width (1% put)']

rows_A = bucket_stats(merged, 'vix_bucket_5',   scheme_labels(SCHEME_A))
rows_B = bucket_stats(merged, 'vix_bucket_9',   scheme_labels(SCHEME_B))
rows_C = bucket_stats(merged, 'vix_bucket_14',  scheme_labels(SCHEME_C))
rows_D = bucket_stats(merged, 'vix_bucket_int', int_bucket_labels(merged))

def scheme_summary(rows, name):
    sizes = [r[1] for r in rows if r[1] > 0]
    n_lt30 = sum(1 for s in sizes if s < 30)
    return [name, len(rows), int(np.median(sizes)) if sizes else 0,
            min(sizes) if sizes else 0, n_lt30]

summary_rows = [
    ['Scheme','# Buckets','Median Sample','Min Sample','Buckets <30 obs'],
    scheme_summary(rows_A,'A (5 buckets)'),
    scheme_summary(rows_B,'B (9 buckets)'),
    scheme_summary(rows_C,'C (14 buckets)'),
    scheme_summary(rows_D,'D (per-integer)'),
]

# ══════════════════════════════════════════════════════════════════════════════
# 4. ANALYSIS 1 — CHART
# ══════════════════════════════════════════════════════════════════════════════
print("Analysis 1: chart…")
fig, ax = plt.subplots(figsize=(14, 6), facecolor=C_DARK)
ax.set_facecolor(C_DARK)

int_lbls = int_bucket_labels(merged)
int_x, int_y, int_n = [], [], []
for lbl in int_lbls:
    sub = merged[merged['vix_bucket_int'] == lbl]
    n   = len(sub)
    if n >= 3:
        r = sub['breach_1pct_put'].sum() / n
        int_x.append(int(lbl))
        int_y.append(r * 100)
        int_n.append(n)

scatter_sizes = [max(20, math.sqrt(n)*4) for n in int_n]
ax.scatter(int_x, int_y, s=scatter_sizes, color='#888888', zorder=3, label='Per-integer rate', alpha=0.8)

def scheme_pooled_rates(df, scheme, bucket_col):
    result = {}
    for lo, hi, name in scheme:
        sub = df[df[bucket_col] == name]
        n   = len(sub)
        r   = sub['breach_1pct_put'].sum() / n if n else 0
        result[name] = (lo, hi, r*100)
    return result

pooled_A = scheme_pooled_rates(merged, SCHEME_A, 'vix_bucket_5')
pooled_B = scheme_pooled_rates(merged, SCHEME_B, 'vix_bucket_9')

for name, (lo, hi, rate) in pooled_A.items():
    x0 = max(lo, 5) if lo != NEG_INF else 5
    x1 = min(hi, 80) if hi != POS_INF else 80
    ax.hlines(rate, x0, x1, colors='#ff4444', linewidth=2.5, zorder=4)

for name, (lo, hi, rate) in pooled_B.items():
    x0 = max(lo, 5) if lo != NEG_INF else 5
    x1 = min(hi, 80) if hi != POS_INF else 80
    ax.hlines(rate, x0, x1, colors='#44ff88', linewidth=2.0, zorder=4, linestyles='dashed')

for boundary in [12, 18, 25, 35]:
    ax.axvline(boundary, color='#ff4444', linewidth=0.8, linestyle=':', alpha=0.6)
for boundary in [12, 14, 16, 18, 21, 25, 30, 40]:
    ax.axvline(boundary, color='#44ff88', linewidth=0.6, linestyle=':', alpha=0.4)

ax.set_xlabel('VIX Level (integer)', color='white', fontsize=11)
ax.set_ylabel('1% Put Breach Rate (%)', color='white', fontsize=11)
ax.set_title('SPY 1% Put Breach Rate by VIX Level — Scheme A vs B Bucket Boundaries', color='white', fontsize=13, fontweight='bold')
ax.tick_params(colors='white')
ax.grid(True, color='#444444', alpha=0.5)
for spine in ax.spines.values(): spine.set_edgecolor('#444444')

legend_handles = [
    mpatches.Patch(color='#888888', label='Per-integer rate (dot size ∝ √days)'),
    plt.Line2D([0],[0], color='#ff4444', linewidth=2.5, label='Scheme A pooled rate (5 buckets)'),
    plt.Line2D([0],[0], color='#44ff88', linewidth=2.0, linestyle='dashed', label='Scheme B pooled rate (9 buckets)'),
]
ax.legend(handles=legend_handles, facecolor='#333333', edgecolor='#555555', labelcolor='white', fontsize=9)
plt.tight_layout()
CHART1 = '/tmp/spy_chart_analysis1.png'
plt.savefig(CHART1, dpi=130, bbox_inches='tight', facecolor=C_DARK)
plt.close()
print(f"  Saved {CHART1}")

# ══════════════════════════════════════════════════════════════════════════════
# 5. ANALYSIS 2 — VIX DIRECTION
# ══════════════════════════════════════════════════════════════════════════════
print("Analysis 2: VIX direction…")
A_LABELS = scheme_labels(SCHEME_A)

def direction_table(df, dir_col, bucket_col, ordered_labels):
    rows = []
    chi_rows = []
    for bkt in ordered_labels:
        sub = df[df[bucket_col] == bkt].dropna(subset=[dir_col])
        r_data = sub[sub[dir_col] == 'Rising']
        f_data = sub[sub[dir_col] == 'Falling']
        nr, nf = len(r_data), len(f_data)
        kr1p = r_data['breach_1pct_put'].sum()
        kf1p = f_data['breach_1pct_put'].sum()
        kr1c = r_data['breach_1pct_call'].sum()
        kf1c = f_data['breach_1pct_call'].sum()
        rr1p = kr1p/nr if nr else 0
        rf1p = kf1p/nf if nf else 0
        rr1c = kr1c/nr if nr else 0
        rf1c = kf1c/nf if nf else 0
        diff = rr1p - rf1p
        rows.append([bkt,'Rising',  nr,  pct(rr1p), pct(rr1c), ''])
        rows.append([bkt,'Falling', nf,  pct(rf1p), pct(rf1c), pct(diff)])
        # chi-squared
        if nr > 0 and nf > 0:
            ct = [[kr1p, nr-kr1p],[kf1p, nf-kf1p]]
            if min(nr, nf) < 10:
                _, p = fisher_exact(ct)
                test = 'Fisher'
            else:
                _, p, _, _ = chi2_contingency(ct)
                test = 'χ²'
        else:
            p, test = 1.0, 'N/A'
        chi_rows.append([bkt, f"{rr1p*100:.1f}%", f"{rf1p*100:.1f}%",
                         f"{diff*100:+.1f}%", f"{p:.4f}", '✓' if p < 0.05 else ''])
    return rows, chi_rows

dir_col_map = {
    'Method 1 (1-day)':   'vix_direction_1d',
    'Method 2 (5-day)':   'vix_direction_5d',
    'Method 3 (10d MA)':  'vix_direction_10dma',
}

all_dir_tables  = {}
all_chi_tables  = {}
winning_method  = None
max_sig_buckets = -1

for method_name, dir_col in dir_col_map.items():
    dt, ct = direction_table(merged, dir_col, 'vix_bucket_5', A_LABELS)
    all_dir_tables[method_name] = dt
    all_chi_tables[method_name] = ct
    sig = sum(1 for r in ct if r[5] == '✓')
    print(f"  {method_name}: {sig}/5 buckets significant")
    if sig > max_sig_buckets:
        max_sig_buckets = sig
        winning_method  = method_name

print(f"  Winning method: {winning_method}")

# ── Analysis 2 Chart ──────────────────────────────────────────────────────────
print("Analysis 2: chart…")
dir_col_m1 = 'vix_direction_1d'
fig, ax = plt.subplots(figsize=(14, 6), facecolor=C_DARK)
ax.set_facecolor(C_DARK)

x_pos = np.arange(len(A_LABELS))
bar_w = 0.35

rise_rates, fall_rates, rise_errs, fall_errs = [], [], [], []
for bkt in A_LABELS:
    sub = merged[merged['vix_bucket_5'] == bkt].dropna(subset=[dir_col_m1])
    r_data = sub[sub[dir_col_m1] == 'Rising']
    f_data = sub[sub[dir_col_m1] == 'Falling']
    nr, nf = len(r_data), len(f_data)
    kr = r_data['breach_1pct_put'].sum()
    kf = f_data['breach_1pct_put'].sum()
    rr = kr/nr if nr else 0
    rf = kf/nf if nf else 0
    rise_rates.append(rr*100)
    fall_rates.append(rf*100)
    lo, hi = wilson_ci(kr, nr)
    rise_errs.append([(rr-lo)*100, (hi-rr)*100])
    lo, hi = wilson_ci(kf, nf)
    fall_errs.append([(rf-lo)*100, (hi-rf)*100])

rise_err_arr = np.array(rise_errs).T
fall_err_arr = np.array(fall_errs).T

bars_r = ax.bar(x_pos - bar_w/2, rise_rates, bar_w, label='Rising VIX',
                color=C_RED, alpha=0.85, yerr=rise_err_arr, error_kw={'ecolor':'white','capsize':4})
bars_f = ax.bar(x_pos + bar_w/2, fall_rates, bar_w, label='Falling VIX',
                color=C_GREEN, alpha=0.85, yerr=fall_err_arr, error_kw={'ecolor':'white','capsize':4})

ax.set_xticks(x_pos)
ax.set_xticklabels(A_LABELS, color='white', fontsize=11)
ax.tick_params(colors='white')
ax.set_xlabel('VIX Bucket', color='white', fontsize=11)
ax.set_ylabel('1% Put Breach Rate (%)', color='white', fontsize=11)
ax.set_title('1% Put Breach Rate: Rising vs Falling VIX — Method 1 (1-Day Change)', color='white', fontsize=13, fontweight='bold')
ax.legend(facecolor='#333333', edgecolor='#555555', labelcolor='white', fontsize=10)
ax.grid(True, axis='y', color='#444444', alpha=0.5)
for spine in ax.spines.values(): spine.set_edgecolor('#444444')
plt.tight_layout()
CHART2 = '/tmp/spy_chart_analysis2.png'
plt.savefig(CHART2, dpi=130, bbox_inches='tight', facecolor=C_DARK)
plt.close()
print(f"  Saved {CHART2}")

# ══════════════════════════════════════════════════════════════════════════════
# 6. ANALYSIS 3 — REGIME DRIFT
# ══════════════════════════════════════════════════════════════════════════════
print("Analysis 3: regime drift…")
cutoff = pd.Timestamp('2020-01-01')
pre  = merged[merged['date'] <  cutoff]
post = merged[merged['date'] >= cutoff]

drift_rows = []
for bkt in A_LABELS:
    pre_s  = pre[pre['vix_bucket_5']  == bkt]
    post_s = post[post['vix_bucket_5'] == bkt]
    np_   = len(pre_s);  kp_ = pre_s['breach_1pct_put'].sum()
    npo   = len(post_s); kpo = post_s['breach_1pct_put'].sum()
    rp_   = kp_/np_  if np_  else 0
    rpo   = kpo/npo  if npo  else 0
    diff  = rpo - rp_
    drift_rows.append([bkt, np_, pct(rp_), npo, pct(rpo), f"{diff*100:+.1f}%"])

# ══════════════════════════════════════════════════════════════════════════════
# 7. RAW CSV
# ══════════════════════════════════════════════════════════════════════════════
print("Saving raw CSV…")
raw = merged[['date','close_spy','spy_return','close_vix',
              'vix_bucket_5','vix_bucket_9',
              'vix_direction_1d','vix_direction_5d','vix_direction_10dma',
              'breach_1pct_put','breach_1pct_call','breach_2pct_put']].copy()
raw.columns = ['date','spy_close','spy_return','vix_close','vix_bucket_5','vix_bucket_9',
               'vix_direction_1d','vix_direction_5d','vix_direction_10dma',
               'breach_1pct_put','breach_1pct_call','breach_2pct_put']
raw.to_csv(os.path.join(OUT_DIR, 'spy_model_analysis_raw.csv'), index=False)
print("  Done")

# ══════════════════════════════════════════════════════════════════════════════
# 8. PDF REPORT
# ══════════════════════════════════════════════════════════════════════════════
print("Building PDF…")
PDF_PATH = os.path.join(OUT_DIR, 'SPY_MODEL_BUCKET_ANALYSIS.pdf')
doc = SimpleDocTemplate(PDF_PATH, pagesize=landscape(letter),
                        leftMargin=0.6*inch, rightMargin=0.6*inch,
                        topMargin=0.6*inch, bottomMargin=0.6*inch)

styles = getSampleStyleSheet()
def sty(name, **kw):
    s = ParagraphStyle(name, parent=styles['Normal'], **kw)
    return s

h1  = sty('H1',  fontSize=22, textColor=colors.white,   backColor=colors.HexColor(C_DARK),
           alignment=TA_CENTER, spaceAfter=8, leading=28, fontName='Helvetica-Bold')
h2  = sty('H2',  fontSize=14, textColor=colors.white,   backColor=colors.HexColor(C_DARK),
           spaceBefore=14, spaceAfter=6, leading=20, fontName='Helvetica-Bold')
sub = sty('SUB', fontSize=11, textColor=colors.HexColor('#888888'), alignment=TA_CENTER, spaceAfter=4)
body= sty('BODY',fontSize=9,  textColor=colors.black, spaceAfter=4, leading=13)

def make_table(header_row, data_rows, col_widths=None):
    all_rows = [header_row] + data_rows
    t = Table(all_rows, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor(C_DARK)),
        ('TEXTCOLOR',  (0,0), (-1,0), colors.white),
        ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0,0), (-1,0), 8),
        ('ALIGN',      (0,0), (-1,-1), 'CENTER'),
        ('VALIGN',     (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, colors.HexColor(C_GREY)]),
        ('FONTSIZE',   (0,1), (-1,-1), 8),
        ('GRID',       (0,0), (-1,-1), 0.3, colors.HexColor('#cccccc')),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING',(0,0),(-1,-1), 3),
    ]
    t.setStyle(TableStyle(style_cmds))
    return t

def section_table(scheme_name, rows):
    hdr = ['Bucket','Days','1% Put Breach','1% Call Breach','2% Put Breach','95% CI Width (1% put)']
    widths = [1.1*inch, 0.7*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.5*inch]
    return make_table(hdr, rows, widths)

story = []

# ── Cover ────────────────────────────────────────────────────────────────────
story.append(Spacer(1, 1.8*inch))
story.append(Paragraph('SPY Daily Move Probability Model', h1))
story.append(Paragraph('Bucket &amp; VIX Direction Analysis', h1))
story.append(Spacer(1, 0.3*inch))
story.append(Paragraph('Angelrow Trading Systems — April 2026', sub))
story.append(Spacer(1, 0.2*inch))
story.append(Paragraph(
    f'Dataset: {len(merged):,} trading days after inner join | '
    f'SPY returns computed close-to-close | VIX bucket classification using 4 schemes', sub))
story.append(PageBreak())

# ── Section 1 ────────────────────────────────────────────────────────────────
story.append(Paragraph('Section 1: Optimal VIX Bucket Count', h2))
story.append(Paragraph(
    'Four bucketing schemes are evaluated: Scheme A (5 buckets, current production), '
    'Scheme B (9 refined buckets), Scheme C (14 fine-grained buckets), and Scheme D '
    '(per-integer VIX). For each bucket the 1% and 2% put/call breach rates are shown '
    'with Wilson 95% confidence interval widths. Buckets with &lt;30 observations are '
    'considered statistically unreliable.', body))
story.append(Spacer(1, 0.1*inch))

for label, rows in [('Scheme A — 5 Buckets (Current)', rows_A),
                     ('Scheme B — 9 Buckets (Refined)', rows_B),
                     ('Scheme C — 14 Buckets (Fine-Grained)', rows_C)]:
    story.append(Paragraph(label, sty('lbl', fontSize=10, fontName='Helvetica-Bold',
                                       spaceBefore=8, spaceAfter=4)))
    story.append(section_table(label, rows))
    story.append(Spacer(1, 0.12*inch))

story.append(Paragraph('Scheme Comparison Summary', sty('lbl2', fontSize=10, fontName='Helvetica-Bold',
                                                          spaceBefore=8, spaceAfter=4)))
sum_widths = [1.8*inch, 1.0*inch, 1.3*inch, 1.1*inch, 1.4*inch]
story.append(make_table(summary_rows[0], summary_rows[1:], sum_widths))
story.append(Spacer(1, 0.2*inch))

story.append(Paragraph('Figure 1 — Per-Integer 1% Put Breach Rate with Scheme A & B Bucket Boundaries', sub))
story.append(Image(CHART1, width=9.5*inch, height=4.1*inch))
story.append(PageBreak())

# ── Section 2 ────────────────────────────────────────────────────────────────
story.append(Paragraph('Section 2: VIX Direction Feature', h2))
story.append(Paragraph(
    'Three methods for classifying VIX direction are evaluated within each Scheme A bucket. '
    'A chi-squared or Fisher\'s exact test determines whether Rising vs Falling VIX produces '
    'statistically different breach rates (p &lt; 0.05). The winning method is highlighted.', body))

dir_hdr = ['VIX Bucket','Direction','Days','1% Put Breach','1% Call Breach','Diff (R−F)']
chi_hdr = ['VIX Bucket','Rising Rate','Falling Rate','Difference','p-value','Sig p<0.05']
d_widths = [0.9*inch, 0.75*inch, 0.65*inch, 1.05*inch, 1.05*inch, 0.9*inch]
c_widths = [0.9*inch, 0.9*inch,  0.9*inch,  0.9*inch,  0.75*inch, 0.75*inch]

for method_name, dir_col in dir_col_map.items():
    is_winner = (method_name == winning_method)
    sig = sum(1 for r in all_chi_tables[method_name] if r[5] == '✓')
    label_txt = f"{method_name}{' ★ WINNING METHOD' if is_winner else ''}"
    lbl_style = sty('ms', fontSize=10, fontName='Helvetica-Bold', spaceBefore=10, spaceAfter=3,
                    textColor=colors.HexColor(C_GREEN if is_winner else '#000000'))
    story.append(Paragraph(label_txt, lbl_style))
    story.append(Paragraph(f'Statistically significant in {sig} of 5 buckets (p &lt; 0.05)', body))
    story.append(make_table(dir_hdr, all_dir_tables[method_name], d_widths))
    story.append(Spacer(1, 0.06*inch))
    story.append(Paragraph('Statistical Test Results:', sty('st', fontSize=9, fontName='Helvetica-Bold')))
    story.append(make_table(chi_hdr, all_chi_tables[method_name], c_widths))
    story.append(Spacer(1, 0.15*inch))

sig_counts = {m: sum(1 for r in all_chi_tables[m] if r[5]=='✓') for m in dir_col_map}
summary_txt = ' | '.join([f"{m}: {s}/5 significant" for m, s in sig_counts.items()])
story.append(Paragraph(f'Summary: {summary_txt}', body))
story.append(Paragraph(
    f'<b>Winning method: {winning_method}</b> — direction adds meaningful information '
    f'in {max_sig_buckets} of 5 VIX buckets.', body))
story.append(Spacer(1, 0.2*inch))

story.append(Paragraph('Figure 2 — 1% Put Breach Rate: Rising vs Falling VIX (Method 1, 1-Day Change)', sub))
story.append(Image(CHART2, width=9.5*inch, height=4.1*inch))
story.append(PageBreak())

# ── Section 3 ────────────────────────────────────────────────────────────────
story.append(Paragraph('Section 3: Regime Drift Check (Pre/Post 2020)', h2))
story.append(Paragraph(
    'The full dataset is split at 1 January 2020. Using Scheme A (5 buckets), '
    '1% put breach rates are compared between the pre-2020 period (~24 years) and '
    'the post-2020 period (~6 years, covering COVID volatility regime and beyond). '
    'Large differences may indicate structural market change requiring recalibration.', body))
story.append(Spacer(1, 0.12*inch))

drift_hdr = ['VIX Bucket','Pre-2020 Days','Pre-2020 Rate','Post-2020 Days','Post-2020 Rate','Difference']
d3_widths  = [1.1*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.0*inch]
story.append(make_table(drift_hdr, drift_rows, d3_widths))
story.append(Spacer(1, 0.3*inch))
story.append(Paragraph(
    'Note: Post-2020 differences &gt; ±5 percentage points warrant investigation before '
    'deploying probability estimates derived from the full historical sample.', body))

doc.build(story)
print(f"PDF saved: {PDF_PATH}")
print(f"PDF size:  {os.path.getsize(PDF_PATH):,} bytes")
print("Done.")
