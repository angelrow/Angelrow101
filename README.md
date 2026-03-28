# Angelrow101

Trading tools, market dashboards, and automation for the Angelrow options strategy.

---

## Morning Trading Briefing

Automated daily email sent at **6:30am UK time (Mon–Fri)** via GitHub Actions.

### What it includes

| Section | Detail |
|---|---|
| **Regime Banner** | VIX-based regime classification (CRISIS / HIGH VOL / ELEVATED / NORMAL / LOW VOL) |
| **Market Snapshot** | VIX, ES, NQ, 10Y Treasury, DXY with price and daily % change |
| **Put/Call Ratio** | Scraped from CBOE with sentiment classification |
| **Strategy Recommendation** | Playbook for the current regime with bullet-point actions |
| **Position Sizing** | 2% risk / 20% notional / 4 positions max — always visible |
| **Economic Calendar** | Today's High/Medium impact events with UK times |
| **Overnight Summary** | 2–3 sentence summary generated from market data |
| **MP3 Audio** | gTTS spoken briefing attached as a ~90-second MP3 |

### Files

```
briefing/
├── main.py                  # Orchestration script
├── requirements.txt         # Python dependencies
└── templates/
    └── email_template.html  # Jinja2 dark-themed HTML email

.github/workflows/
└── morning-briefing.yml     # GitHub Actions cron workflow
```

### GitHub Secrets required

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Value |
|---|---|
| `SENDER_EMAIL` | Gmail address used to send (e.g. `angelrow.briefing@gmail.com`) |
| `SENDER_PASSWORD` | Gmail **App Password** — not your account password |
| `RECIPIENT_EMAIL` | Email address to deliver the briefing to |

> **Gmail App Password**: Google Account → Security → 2-Step Verification → App Passwords

### VIX Regime Playbook

| VIX | Regime | Strategy |
|---|---|---|
| < 12 | LOW VOL | Iron condors, calendars, reduced size |
| 12–18 | NORMAL | Standard CSPs, 3–5 day holds |
| 18–25 | ELEVATED | High IV-rank CSPs, iron condors |
| 25–35 | HIGH VOL | Aggressive CSPs, bull put spreads, 0–2 day holds |
| 35+ | CRISIS | Cash only, protective puts |

### Manual trigger

Run anytime from **Actions → Morning Trading Briefing → Run workflow** — the time gate is bypassed for manual runs.

---

## Market Weather Grid

Live market dashboard updating every 15 minutes during US market hours.
→ [market-weather-grid.html](market-weather-grid.html)

---

## Other Tools

- [CSP Checklist](Csp_checklist.html) — pre-trade options checklist
- [Indicators](indicators.html) — technical indicator reference
- [Monte Carlo](monte-carlo.js) — position simulation
- [Trailing Stop](trailing-stop.html) — stop-loss calculator
