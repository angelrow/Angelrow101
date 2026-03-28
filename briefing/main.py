#!/usr/bin/env python3
"""
Angelrow Morning Trading Briefing
Sends a daily market briefing email at 6:30am UK time with an MP3 audio attachment.

Secrets required (GitHub Actions → Settings → Secrets):
  SENDER_EMAIL      Gmail address used to send
  SENDER_PASSWORD   Gmail App Password (not your account password)
  RECIPIENT_EMAIL   Where the briefing is delivered
"""

import logging
import os
import smtplib
import sys
import tempfile
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pytz
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from gtts import gTTS
from jinja2 import Environment, FileSystemLoader

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("briefing")

UK_TZ = pytz.timezone("Europe/London")
HERE = Path(__file__).parent

# ── Time gate ────────────────────────────────────────────────────────────────


def is_within_send_window() -> bool:
    """
    Allow ±40 min around 06:30 UK time.
    Bypass for manual workflow_dispatch triggers.
    """
    if os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch":
        log.info("Manual trigger — skipping time gate.")
        return True
    now = datetime.now(UK_TZ)
    target = now.replace(hour=6, minute=30, second=0, microsecond=0)
    delta_s = abs((now - target).total_seconds())
    in_window = delta_s <= 2400  # 40 minutes
    log.info(
        "Time gate: UK time %s, delta %.0fs from 06:30, in_window=%s",
        now.strftime("%H:%M"),
        delta_s,
        in_window,
    )
    return in_window


# ── Market data ──────────────────────────────────────────────────────────────

TICKERS = [
    ("^VIX", "VIX"),
    ("ES=F", "S&P 500 Futures"),
    ("NQ=F", "Nasdaq 100 Futures"),
    ("^TNX", "10Y Treasury"),
    ("DX-Y.NYB", "DXY Dollar"),
]


def fetch_ticker(symbol: str, label: str) -> dict:
    """Fetch latest close and daily % change for a ticker."""
    try:
        hist = yf.Ticker(symbol).history(period="5d")
        if len(hist) < 2:
            raise ValueError(f"Only {len(hist)} rows returned")
        prev = float(hist["Close"].iloc[-2])
        curr = float(hist["Close"].iloc[-1])
        chg = (curr - prev) / prev * 100
        return {
            "symbol": symbol,
            "label": label,
            "value": curr,
            "prev": prev,
            "change_pct": chg,
            "arrow": "↑" if chg > 0.05 else ("↓" if chg < -0.05 else "→"),
            "arrow_color": (
                "#4CAF50" if chg > 0.05 else ("#FF5252" if chg < -0.05 else "#9E9E9E")
            ),
            "ok": True,
        }
    except Exception as exc:
        log.error("Ticker %s failed: %s", symbol, exc)
        return {"symbol": symbol, "label": label, "ok": False, "error": str(exc)}


# ── VIX regime classification ─────────────────────────────────────────────────


def classify_vix(vix_value: float) -> dict:
    if vix_value >= 35:
        return {
            "regime": "CRISIS",
            "color": "#FF3B3B",
            "bg": "#2d0000",
            "strategy": "Capital preservation only. Sit in cash or buy protective puts.",
            "detail": [
                "No new short-premium positions",
                "Close open positions where possible",
                "Long OTM puts as portfolio insurance",
                "Hedge with inverse ETFs if already positioned",
            ],
        }
    elif vix_value >= 25:
        return {
            "regime": "HIGH VOL",
            "color": "#FF8C00",
            "bg": "#2d1a00",
            "strategy": "Aggressive CSPs, bull put spreads, short holds (0–2 days).",
            "detail": [
                "Short-dated CSPs with wide strikes",
                "Bull put spreads on strong technical support",
                "Collect premium aggressively — IV is rich",
                "0–2 day hold targets maximum",
            ],
        }
    elif vix_value >= 18:
        return {
            "regime": "ELEVATED",
            "color": "#FFD700",
            "bg": "#2d2700",
            "strategy": "CSPs on high-IV-rank names, iron condors on range-bound underlyings.",
            "detail": [
                "CSPs on stocks with IV rank > 50",
                "Iron condors on confirmed sideways movers",
                "Wider spreads to account for larger moves",
                "2–4 day hold targets",
            ],
        }
    elif vix_value >= 12:
        return {
            "regime": "NORMAL",
            "color": "#4CAF50",
            "bg": "#002d0a",
            "strategy": "Standard premium selling with 3–5 day holds.",
            "detail": [
                "Standard CSPs at 30-delta strikes",
                "Begin adding iron condors to the mix",
                "3–5 day hold targets",
                "Scale into positions gradually",
            ],
        }
    else:
        return {
            "regime": "LOW VOL",
            "color": "#00BCD4",
            "bg": "#002d2d",
            "strategy": "Iron condors, calendar spreads, reduced size. Anticipate a spike.",
            "detail": [
                "Iron condors on range-bound names",
                "Calendar spreads for theta decay",
                "Reduce overall position size",
                "Avoid naked short premium — spike risk is elevated",
            ],
        }


# ── Put / Call ratio ──────────────────────────────────────────────────────────


def fetch_put_call_ratio() -> dict:
    """Scrape CBOE equity put/call ratio from their daily stats page."""
    try:
        url = "https://www.cboe.com/us/options/market_statistics/daily/"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # CBOE table contains labelled rows — scan for equity P/C
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                row_text = " ".join(cells).lower()
                if "equity" in row_text or ("put" in row_text and "call" in row_text):
                    for cell in cells:
                        try:
                            val = float(cell)
                            if 0.30 <= val <= 3.00:
                                if val > 1.0:
                                    sentiment = "Bearish"
                                elif val < 0.70:
                                    sentiment = "Bullish"
                                else:
                                    sentiment = "Neutral"
                                return {"value": val, "sentiment": sentiment, "ok": True}
                        except ValueError:
                            pass
        raise ValueError("P/C ratio not found in page")
    except Exception as exc:
        log.warning("Put/Call ratio fetch failed: %s", exc)
        return {"ok": False, "error": str(exc)}


# ── Economic calendar ─────────────────────────────────────────────────────────


def fetch_economic_calendar() -> list:
    """
    Fetch today's medium/high impact economic events from the
    Forex Factory public JSON feed.
    """
    events = []
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            raise ValueError(f"HTTP {resp.status_code}")
        data = resp.json()

        today_str = datetime.now(UK_TZ).strftime("%Y-%m-%d")
        for event in data:
            if event.get("impact") not in ("High", "Medium"):
                continue
            raw_date = event.get("date", "")
            if not raw_date.startswith(today_str):
                continue
            # FF calendar timestamps are UTC
            try:
                dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                time_str = dt.astimezone(UK_TZ).strftime("%H:%M")
            except Exception:
                time_str = raw_date[11:16] if len(raw_date) >= 16 else "TBA"

            events.append(
                {
                    "time": time_str,
                    "currency": event.get("country", event.get("currency", "")).upper(),
                    "title": event.get("title", "Unknown Event"),
                    "impact": event.get("impact", ""),
                    "forecast": event.get("forecast") or "–",
                    "previous": event.get("previous") or "–",
                }
            )
        events.sort(key=lambda x: x["time"])
        log.info("Calendar: %d events for %s", len(events), today_str)
    except Exception as exc:
        log.warning("Economic calendar fetch failed: %s", exc)
    return events


# ── Overnight summary ─────────────────────────────────────────────────────────


def build_overnight_summary(market_data: dict) -> str:
    """Compose a short 2-3 sentence overnight summary from fetched data."""
    sentences = []

    es = market_data.get("ES=F", {})
    nq = market_data.get("NQ=F", {})
    vix = market_data.get("^VIX", {})
    tnx = market_data.get("^TNX", {})
    dxy = market_data.get("DX-Y.NYB", {})

    if es.get("ok") and nq.get("ok"):
        es_dir = "higher" if es["change_pct"] > 0 else "lower"
        nq_dir = "higher" if nq["change_pct"] > 0 else "lower"
        sentences.append(
            f"US equity futures are trading {es_dir} overnight: S&P 500 E-minis "
            f"{es['arrow']} {abs(es['change_pct']):.2f}% and Nasdaq 100 E-minis "
            f"{nq['arrow']} {abs(nq['change_pct']):.2f}%."
        )
    elif es.get("ok"):
        dir_ = "higher" if es["change_pct"] > 0 else "lower"
        sentences.append(
            f"S&P 500 E-mini futures are trading {dir_} ({es['arrow']} {abs(es['change_pct']):.2f}%)."
        )

    if vix.get("ok") and tnx.get("ok"):
        vix_move = "rising" if vix["change_pct"] > 0 else "easing"
        bond_move = "rising" if tnx["change_pct"] > 0 else "falling"
        sentences.append(
            f"Volatility is {vix_move} with VIX at {vix['value']:.2f}, "
            f"while 10-year Treasury yields are {bond_move} at {tnx['value']:.3f}%."
        )

    if dxy.get("ok"):
        dxy_move = "strengthening" if dxy["change_pct"] > 0 else "weakening"
        sentences.append(
            f"The US Dollar is {dxy_move} against major currencies "
            f"(DXY {dxy['arrow']} {abs(dxy['change_pct']):.2f}%)."
        )

    if not sentences:
        return "Market data unavailable for overnight summary."
    return " ".join(sentences)


# ── Audio script ──────────────────────────────────────────────────────────────


def build_audio_script(
    market_data: dict, regime: dict, events: list, now: datetime
) -> str:
    """Concise spoken briefing — target under 2 minutes."""
    date_str = now.strftime("%A, %B %d, %Y")
    vix = market_data.get("^VIX", {})
    es = market_data.get("ES=F", {})
    nq = market_data.get("NQ=F", {})
    tnx = market_data.get("^TNX", {})
    dxy = market_data.get("DX-Y.NYB", {})

    lines = [f"Good morning. Angelrow trading briefing for {date_str}.", ""]

    # Regime
    if vix.get("ok"):
        direction = "up" if vix["change_pct"] > 0 else "down"
        lines.append(
            f"Market regime: {regime['regime']}. "
            f"VIX is at {vix['value']:.2f}, {direction} {abs(vix['change_pct']):.1f} percent from yesterday."
        )
        lines.append("")

    # Snapshot
    lines.append("Market snapshot.")
    for d in [es, nq, tnx, dxy]:
        if d.get("ok"):
            direction = "up" if d["change_pct"] > 0 else "down"
            lines.append(
                f"{d['label']}: {d['value']:.2f}, {direction} {abs(d['change_pct']):.2f} percent."
            )
    lines.append("")

    # Strategy
    lines.append(f"Strategy: {regime['strategy']}")
    lines.append(
        "Position sizing rules: maximum 2 percent portfolio risk per trade, "
        "20 percent maximum notional exposure, and no more than 4 simultaneous positions."
    )
    lines.append("")

    # Events
    if events:
        lines.append("Key economic events today:")
        for e in events[:5]:
            lines.append(
                f"  {e['time']} UK time — {e['title']}, {e['impact']} impact, {e['currency']}."
            )
    else:
        lines.append(
            "No high-impact economic events found for today. "
            "Check Investing dot com for the full calendar."
        )
    lines.append("")
    lines.append("That is your morning briefing. Trade well, and manage your risk.")

    return "\n".join(lines)


def generate_audio(script: str) -> str | None:
    """Generate MP3 via gTTS and return the temp file path (caller must delete)."""
    try:
        tts = gTTS(text=script, lang="en", tld="co.uk", slow=False)
        tmp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
        tts.save(tmp.name)
        size_kb = Path(tmp.name).stat().st_size // 1024
        log.info("Audio generated: %s (%d KB)", tmp.name, size_kb)
        return tmp.name
    except Exception as exc:
        log.error("Audio generation failed: %s", exc)
        return None


# ── Email rendering ───────────────────────────────────────────────────────────


def build_html_email(
    market_data: dict,
    regime: dict,
    events: list,
    pc_ratio: dict,
    overnight: str,
    now: datetime,
) -> str:
    env = Environment(
        loader=FileSystemLoader(HERE / "templates"),
        autoescape=True,
    )
    template = env.get_template("email_template.html")

    snapshot = [
        market_data[sym]
        for sym in ["^VIX", "ES=F", "NQ=F", "^TNX", "DX-Y.NYB"]
        if sym in market_data
    ]

    return template.render(
        date_str=now.strftime("%A, %d %B %Y"),
        time_str=now.strftime("%H:%M %Z"),
        regime=regime,
        vix_data=market_data.get("^VIX", {"ok": False}),
        snapshot=snapshot,
        events=events,
        pc_ratio=pc_ratio,
        overnight=overnight,
    )


def build_plain_text(
    market_data: dict, regime: dict, events: list, overnight: str, now: datetime
) -> str:
    sep = "=" * 60
    thin = "-" * 40
    lines = [
        f"ANGELROW MORNING BRIEFING — {now.strftime('%A, %d %B %Y  %H:%M %Z')}",
        sep,
        "",
        f"MARKET REGIME: {regime['regime']}",
    ]
    vix = market_data.get("^VIX", {})
    if vix.get("ok"):
        lines.append(f"VIX: {vix['value']:.2f}  {vix['arrow']}  {vix['change_pct']:+.2f}%")
    lines += ["", "MARKET SNAPSHOT", thin]
    for sym in ["^VIX", "ES=F", "NQ=F", "^TNX", "DX-Y.NYB"]:
        d = market_data.get(sym, {})
        if d.get("ok"):
            lines.append(
                f"  {d['label']:<26} {d['value']:>10.3f}  "
                f"{d['arrow']}  {d['change_pct']:+.2f}%"
            )
        else:
            lines.append(f"  {d.get('label', sym):<26} {'N/A':>10}")
    if pc_ratio := market_data.get("_pc_ratio"):
        if pc_ratio.get("ok"):
            lines.append(f"\n  Put/Call Ratio: {pc_ratio['value']:.2f}  ({pc_ratio['sentiment']})")

    lines += [
        "",
        "STRATEGY",
        thin,
        regime["strategy"],
        "",
    ]
    for point in regime["detail"]:
        lines.append(f"  ▸ {point}")
    lines += [
        "",
        "  Position sizing: 2% max risk/trade | 20% max notional | 4 positions max",
        "",
        "ECONOMIC CALENDAR",
        thin,
    ]
    if events:
        for e in events:
            lines.append(
                f"  {e['time']}  [{e['impact']:6}]  {e['currency']:<4}  {e['title']}"
            )
    else:
        lines.append(
            "  No high-impact events found.\n"
            "  Full calendar: https://www.investing.com/economic-calendar/"
        )
    lines += ["", "OVERNIGHT SUMMARY", thin, overnight, ""]
    return "\n".join(lines)


# ── Email sending ─────────────────────────────────────────────────────────────


def send_email(
    html_body: str,
    text_body: str,
    audio_path: str | None,
    now: datetime,
) -> None:
    sender = os.environ["SENDER_EMAIL"]
    password = os.environ["SENDER_PASSWORD"]
    recipient = os.environ["RECIPIENT_EMAIL"]

    date_str = now.strftime("%a %d %b %Y")
    subject = f"[Angelrow] Morning Briefing — {date_str}"

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = f"Angelrow Briefing <{sender}>"
    msg["To"] = recipient

    # text + html alternative block
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    # MP3 attachment
    if audio_path:
        try:
            with open(audio_path, "rb") as fh:
                audio_bytes = fh.read()
            part = MIMEBase("audio", "mpeg")
            part.set_payload(audio_bytes)
            encoders.encode_base64(part)
            fname = f"angelrow_briefing_{now.strftime('%Y%m%d')}.mp3"
            part.add_header("Content-Disposition", "attachment", filename=fname)
            msg.attach(part)
            log.info("Audio attached: %s (%d KB)", fname, len(audio_bytes) // 1024)
        except Exception as exc:
            log.error("Failed to attach audio: %s", exc)

    log.info("Sending to %s via Gmail SMTP…", recipient)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.sendmail(sender, recipient, msg.as_string())
    log.info("Email sent successfully.")


# ── Orchestration ─────────────────────────────────────────────────────────────


def main() -> None:
    now = datetime.now(UK_TZ)
    log.info(
        "Angelrow morning briefing — %s", now.strftime("%Y-%m-%d %H:%M %Z")
    )

    if not is_within_send_window():
        log.info("Outside send window — exiting without sending.")
        sys.exit(0)

    # ── Fetch market data ────────────────────────────────────────
    log.info("Fetching market data…")
    market_data: dict = {}
    for symbol, label in TICKERS:
        log.info("  Fetching %s (%s)…", symbol, label)
        market_data[symbol] = fetch_ticker(symbol, label)

    # VIX regime
    vix_data = market_data.get("^VIX", {})
    vix_value = vix_data.get("value", 20.0) if vix_data.get("ok") else 20.0
    regime = classify_vix(vix_value)
    log.info("Regime: %s (VIX %.2f)", regime["regime"], vix_value)

    # Put/Call ratio
    log.info("Fetching Put/Call ratio…")
    pc_ratio = fetch_put_call_ratio()
    if pc_ratio.get("ok"):
        log.info("P/C ratio: %.2f (%s)", pc_ratio["value"], pc_ratio["sentiment"])

    # Economic calendar
    log.info("Fetching economic calendar…")
    events = fetch_economic_calendar()

    # Overnight summary
    overnight = build_overnight_summary(market_data)

    # Build bodies
    log.info("Rendering email…")
    html_body = build_html_email(market_data, regime, events, pc_ratio, overnight, now)
    text_body = build_plain_text(market_data, regime, events, overnight, now)

    # Audio
    log.info("Generating audio briefing…")
    audio_script = build_audio_script(market_data, regime, events, now)
    audio_path = generate_audio(audio_script)

    # Send
    send_email(html_body, text_body, audio_path, now)

    # Cleanup temp file
    if audio_path:
        try:
            Path(audio_path).unlink(missing_ok=True)
        except Exception:
            pass

    log.info("Done.")


if __name__ == "__main__":
    main()
