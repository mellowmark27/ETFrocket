"""
MomentumLens — Phase 4: Alert Engine
======================================
Manages watchlists and sends email notifications for:

  1. Daily Digest       — Morning summary of all current candidates
  2. Signal Flip Alert  — Immediate notification when abs momentum flips
  3. Threshold Alert    — When a watched ETF crosses a score threshold
  4. Top-N Alert        — When a watched ETF enters the top N of its peer group

Alert subscriptions are stored in the SQLite database (alert_subscriptions table).
Emails are sent via SMTP (Gmail, Outlook, or any SMTP relay).

Configuration via environment variables or a .env file:
    ML_SMTP_HOST     — SMTP server host (default: smtp.gmail.com)
    ML_SMTP_PORT     — SMTP port (default: 587)
    ML_SMTP_USER     — SMTP username / sender address
    ML_SMTP_PASS     — SMTP password or app password
    ML_ALERT_FROM    — From address (defaults to ML_SMTP_USER)
    ML_ALERT_TO      — Default recipient email (can be overridden per subscription)

Usage:
    # Send daily digest to a recipient
    python alerts.py digest --to you@example.com

    # Check and send any triggered alerts
    python alerts.py check

    # Add a watchlist item
    python alerts.py watch --isin IE00B4L5Y983 --email you@example.com --threshold 70

    # List all watched ETFs
    python alerts.py list

    # Test SMTP config (sends a test email)
    python alerts.py test --to you@example.com
"""

import os
import sys
import smtplib
import logging
import argparse
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from database import (
    get_candidates,
    get_signal_flips,
    get_score_history,
    get_latest_scores,
    get_conn,
    init_db,
    DEFAULT_DB_PATH,
)

log = logging.getLogger("momentum_lens.alerts")

# ─────────────────────────────────────────────────────────
# DB SCHEMA EXTENSION — alert_subscriptions table
# ─────────────────────────────────────────────────────────

ALERT_SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_subscriptions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL,
    isin            TEXT,           -- NULL = global / digest subscription
    alert_type      TEXT NOT NULL,  -- 'digest' | 'flip' | 'threshold' | 'top_n'
    threshold       REAL,           -- for alert_type='threshold': dual_score min
    top_n           INTEGER,        -- for alert_type='top_n': rank within peer group
    lookback_months INTEGER NOT NULL DEFAULT 12,
    active          INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    last_triggered  TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS alert_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sent_at         TEXT NOT NULL,
    email           TEXT NOT NULL,
    alert_type      TEXT NOT NULL,
    subject         TEXT,
    isin            TEXT,
    status          TEXT,           -- 'sent' | 'failed' | 'skipped'
    error_message   TEXT
);
"""


def ensure_alert_tables(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Add alert tables if they don't exist (idempotent)."""
    with get_conn(db_path) as conn:
        conn.executescript(ALERT_SCHEMA)


# ─────────────────────────────────────────────────────────
# SMTP CONFIG
# ─────────────────────────────────────────────────────────

class SMTPConfig:
    def __init__(self):
        self.host     = os.getenv("ML_SMTP_HOST", "smtp.gmail.com")
        self.port     = int(os.getenv("ML_SMTP_PORT", "587"))
        self.user     = os.getenv("ML_SMTP_USER", "")
        self.password = os.getenv("ML_SMTP_PASS", "")
        self.from_addr = os.getenv("ML_ALERT_FROM", self.user)

    @property
    def is_configured(self) -> bool:
        return bool(self.user and self.password)

    def __repr__(self):
        return f"SMTPConfig(host={self.host}:{self.port}, user={self.user or '(not set)'})"


# ─────────────────────────────────────────────────────────
# EMAIL SENDING
# ─────────────────────────────────────────────────────────

def send_email(
    to: str,
    subject: str,
    html_body: str,
    config: Optional[SMTPConfig] = None,
) -> bool:
    """
    Send an HTML email via SMTP.
    Returns True on success, False on failure.
    """
    cfg = config or SMTPConfig()

    if not cfg.is_configured:
        log.warning(
            "SMTP not configured. Set ML_SMTP_USER and ML_SMTP_PASS environment variables.\n"
            "  For Gmail: use an App Password (not your main password).\n"
            "  See: https://support.google.com/accounts/answer/185833"
        )
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg.from_addr
    msg["To"]      = to

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(cfg.host, cfg.port, timeout=15) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(cfg.user, cfg.password)
            smtp.sendmail(cfg.from_addr, to, msg.as_string())
        log.info(f"Email sent: '{subject}' → {to}")
        return True
    except Exception as e:
        log.error(f"Email failed: {e}")
        return False


def log_alert(
    email: str,
    alert_type: str,
    subject: str,
    status: str,
    isin: Optional[str] = None,
    error: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Record a sent (or failed) alert to the audit log."""
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO alert_log (sent_at, email, alert_type, subject, isin, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now().isoformat(timespec="seconds"), email, alert_type, subject, isin, status, error),
        )


# ─────────────────────────────────────────────────────────
# EMAIL TEMPLATES
# ─────────────────────────────────────────────────────────

EMAIL_CSS = """
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0d1117; color: #c8d8e8; margin: 0; padding: 0; }
  .wrapper { max-width: 680px; margin: 0 auto; padding: 24px 16px; }
  .header { border-bottom: 1px solid #1c2a3a; padding-bottom: 20px; margin-bottom: 24px; }
  .logo { font-size: 20px; font-weight: 800; color: #e8f2fa; letter-spacing: -0.02em; }
  .logo span { color: #00e5a0; }
  .subtitle { font-size: 12px; color: #4a6070; margin-top: 4px; font-family: monospace; }
  .section-title { font-size: 10px; letter-spacing: 0.18em; text-transform: uppercase;
                   color: #4a6070; font-family: monospace; margin-bottom: 12px; }
  .etf-card { background: #111820; border: 1px solid #1c2a3a; padding: 14px 16px;
              margin-bottom: 8px; }
  .etf-card.strong  { border-left: 3px solid #00e5a0; }
  .etf-card.moderate { border-left: 3px solid #f5a623; }
  .etf-card.flip-on  { border-left: 3px solid #00e5a0; background: rgba(0,229,160,0.04); }
  .etf-card.flip-off { border-left: 3px solid #e8524a; background: rgba(232,82,74,0.04); }
  .etf-name { font-size: 14px; font-weight: 600; color: #e8f2fa; margin-bottom: 2px; }
  .etf-ticker { font-family: monospace; font-size: 11px; color: #00e5a0; }
  .etf-meta { font-size: 11px; color: #4a6070; margin-top: 2px; }
  .score-row { display: flex; gap: 24px; margin-top: 10px; flex-wrap: wrap; }
  .score-item { }
  .score-val { font-family: monospace; font-size: 16px; font-weight: 600; color: #e8f2fa; }
  .score-val.green { color: #00e5a0; }
  .score-val.amber { color: #f5a623; }
  .score-val.red   { color: #e8524a; }
  .score-lbl { font-size: 10px; color: #4a6070; letter-spacing: 0.06em; text-transform: uppercase; }
  .flip-badge { display: inline-block; font-family: monospace; font-size: 11px;
                padding: 3px 8px; font-weight: 600; }
  .flip-badge.on  { color: #00e5a0; background: rgba(0,229,160,0.1);
                    border: 1px solid rgba(0,229,160,0.3); }
  .flip-badge.off { color: #e8524a; background: rgba(232,82,74,0.1);
                    border: 1px solid rgba(232,82,74,0.3); }
  .divider { border: none; border-top: 1px solid #1c2a3a; margin: 24px 0; }
  .footer { font-size: 10px; color: #4a6070; line-height: 1.7; padding-top: 20px;
            border-top: 1px solid #1c2a3a; font-family: monospace; }
  .rf-box { background: #111820; border: 1px solid #1c2a3a; padding: 12px 16px;
            margin-bottom: 20px; font-family: monospace; font-size: 12px;
            display: flex; gap: 32px; }
  .rf-item .val { font-size: 18px; color: #4a9eff; font-weight: 600; }
  .rf-item .lbl { font-size: 10px; color: #4a6070; text-transform: uppercase; letter-spacing: 0.08em; }
  .empty-note { color: #4a6070; font-size: 13px; padding: 20px 0; font-style: italic; }
  a { color: #00e5a0; }
"""

def _etf_card_html(etf: dict, card_class: str = "") -> str:
    dual  = etf.get("dual_score", 0) or 0
    rel   = etf.get("rel_score")
    ret12 = etf.get("ret_12m")
    ret6  = etf.get("ret_6m")
    abs_s = etf.get("abs_signal", 0)
    signal = etf.get("signal_class", "")

    def pct(v): return f"{'+'if v>0 else ''}{v:.1f}%" if v is not None else "—"
    def color(v, force=None):
        if force: return force
        return "green" if (v or 0) > 0 else "red" if (v or 0) < 0 else ""

    return f"""
    <div class="etf-card {card_class}">
      <div class="etf-name">{etf.get('name','')}</div>
      <div>
        <span class="etf-ticker">{etf.get('ticker','')}</span>
        <span class="etf-meta"> · {etf.get('asset_class','')} · {etf.get('region','')}</span>
      </div>
      <div class="score-row">
        <div class="score-item">
          <div class="score-val green">{dual:.1f}</div>
          <div class="score-lbl">Dual Score</div>
        </div>
        <div class="score-item">
          <div class="score-val">{f'{rel:.1f}' if rel is not None else '—'}</div>
          <div class="score-lbl">Rel Rank</div>
        </div>
        <div class="score-item">
          <div class="score-val {color(None, 'green' if abs_s else 'red')}"
               >{"ON" if abs_s else "OFF"}</div>
          <div class="score-lbl">Abs Momentum</div>
        </div>
        <div class="score-item">
          <div class="score-val {color(ret12)}">{pct(ret12)}</div>
          <div class="score-lbl">12M Return</div>
        </div>
        <div class="score-item">
          <div class="score-val {color(ret6)}">{pct(ret6)}</div>
          <div class="score-lbl">6M Return</div>
        </div>
        <div class="score-item">
          <div class="score-val">{etf.get('aum_gbp_m', 0) and f"£{etf['aum_gbp_m']:,.0f}M" or '—'}</div>
          <div class="score-lbl">AUM</div>
        </div>
      </div>
    </div>"""


def build_digest_email(
    candidates: pd.DataFrame,
    rf_rate: float,
    snap_date: str,
    lookback_months: int = 12,
) -> tuple[str, str]:
    """
    Build the daily digest email.
    Returns (subject, html_body).
    """
    n_strong   = int((candidates["signal_class"] == "Strong Candidate").sum())
    n_moderate = int((candidates["signal_class"] == "Moderate Candidate").sum())
    n_total    = len(candidates)

    subject = (
        f"MomentumLens Digest — {snap_date} · "
        f"{n_strong} Strong / {n_moderate} Moderate Candidates"
    )

    cards_strong = ""
    cards_moderate = ""

    for _, row in candidates.iterrows():
        etf = row.to_dict()
        if etf.get("signal_class") == "Strong Candidate":
            cards_strong += _etf_card_html(etf, "strong")
        elif etf.get("signal_class") == "Moderate Candidate":
            cards_moderate += _etf_card_html(etf, "moderate")

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>{EMAIL_CSS}</style></head><body>
<div class="wrapper">
  <div class="header">
    <div class="logo">Momentum<span>Lens</span></div>
    <div class="subtitle">LSE ETF Dual Momentum Scanner · Daily Digest</div>
  </div>

  <div class="rf-box">
    <div class="rf-item">
      <div class="val">{rf_rate:.2f}%</div>
      <div class="lbl">Risk-free Rate (3M Gilt)</div>
    </div>
    <div class="rf-item">
      <div class="val">{n_total}</div>
      <div class="lbl">Total Candidates (≥70)</div>
    </div>
    <div class="rf-item">
      <div class="val">{lookback_months}-1M</div>
      <div class="lbl">Lookback Window</div>
    </div>
    <div class="rf-item">
      <div class="val">{snap_date}</div>
      <div class="lbl">Data Snapshot</div>
    </div>
  </div>

  <div class="section-title">Strong Candidates — Dual Score ≥ 80</div>
  {cards_strong if cards_strong else '<p class="empty-note">No strong candidates today.</p>'}

  <hr class="divider">
  <div class="section-title">Moderate Candidates — Dual Score 70–79</div>
  {cards_moderate if cards_moderate else '<p class="empty-note">No moderate candidates today.</p>'}

  <div class="footer">
    ⚠ For research purposes only. Not financial advice. Past momentum signals do not
    guarantee future performance. Always conduct your own research before investing.
    <br><br>
    Data sourced from JustETF · Generated by MomentumLens · {datetime.now().strftime('%Y-%m-%d %H:%M')}
    <br>
    To unsubscribe: run <code>python alerts.py unwatch --email you@example.com</code>
  </div>
</div></body></html>"""

    return subject, html


def build_flip_email(flips: pd.DataFrame, snap_date: str) -> tuple[str, str]:
    """
    Build a signal flip notification email.
    Returns (subject, html_body).
    """
    n_on  = int((flips["new_signal"] == 1).sum())
    n_off = int((flips["new_signal"] == 0).sum())

    subject = f"MomentumLens ⚡ Signal Alert — {n_on} ON / {n_off} OFF · {snap_date}"

    cards = ""
    for _, row in flips.iterrows():
        is_on = row["new_signal"] == 1
        direction = "↑ OFF → ON" if is_on else "↓ ON → OFF"
        cls = "flip-on" if is_on else "flip-off"
        badge_cls = "on" if is_on else "off"
        cards += f"""
        <div class="etf-card {cls}">
          <div style="margin-bottom:8px;">
            <span class="flip-badge {badge_cls}">{direction}</span>
          </div>
          <div class="etf-name">{row.get('name','')}</div>
          <div>
            <span class="etf-ticker">{row.get('ticker','')}</span>
            <span class="etf-meta"> · {row.get('asset_class','')} · ISIN: {row['isin']}</span>
          </div>
          <div class="score-row">
            <div class="score-item">
              <div class="score-val {'green' if is_on else 'red'}">{(row.get('dual_score') or 0):.1f}</div>
              <div class="score-lbl">Dual Score</div>
            </div>
            <div class="score-item">
              <div class="score-val">{row['flip_date']}</div>
              <div class="score-lbl">Flip Date</div>
            </div>
          </div>
        </div>"""

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>{EMAIL_CSS}</style></head><body>
<div class="wrapper">
  <div class="header">
    <div class="logo">Momentum<span>Lens</span></div>
    <div class="subtitle">LSE ETF Dual Momentum Scanner · Signal Alert</div>
  </div>
  <p style="font-size:13px;margin-bottom:20px;color:#c8d8e8;">
    The following ETFs changed their absolute momentum signal since the last check.
    <strong style="color:#e8f2fa;">OFF → ON</strong> means the ETF has crossed above the risk-free rate —
    a potential entry signal. <strong style="color:#e8f2fa;">ON → OFF</strong> means it has dropped below —
    a potential exit signal.
  </p>
  {cards}
  <div class="footer">
    ⚠ For research purposes only. Not financial advice.
    <br><br>
    Generated by MomentumLens · {datetime.now().strftime('%Y-%m-%d %H:%M')}
  </div>
</div></body></html>"""

    return subject, html


def build_threshold_email(etf: dict, old_score: float, new_score: float) -> tuple[str, str]:
    """Build an alert for a single ETF crossing a score threshold."""
    direction = "↑" if new_score > old_score else "↓"
    subject = (
        f"MomentumLens {direction} Threshold Alert — "
        f"{etf.get('ticker',etf.get('isin'))} score {new_score:.1f}"
    )
    card = _etf_card_html(etf, "strong" if new_score >= 80 else "moderate")
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>{EMAIL_CSS}</style></head><body>
<div class="wrapper">
  <div class="header">
    <div class="logo">Momentum<span>Lens</span></div>
    <div class="subtitle">LSE ETF Dual Momentum Scanner · Threshold Alert</div>
  </div>
  <p style="font-size:13px;margin-bottom:20px;color:#c8d8e8;">
    A watched ETF has crossed your score threshold.
    Previous score: <strong style="color:#f5a623;">{old_score:.1f}</strong> →
    New score: <strong style="color:#00e5a0;">{new_score:.1f}</strong>
  </p>
  {card}
  <div class="footer">
    ⚠ For research purposes only. Not financial advice.<br><br>
    Generated by MomentumLens · {datetime.now().strftime('%Y-%m-%d %H:%M')}
  </div>
</div></body></html>"""
    return subject, html


# ─────────────────────────────────────────────────────────
# WATCHLIST MANAGEMENT
# ─────────────────────────────────────────────────────────

def add_watch(
    email: str,
    alert_type: str = "digest",
    isin: Optional[str] = None,
    threshold: Optional[float] = 70.0,
    top_n: Optional[int] = None,
    lookback_months: int = 12,
    notes: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """
    Add an alert subscription. Returns the new subscription ID.

    alert_type options:
      'digest'    — daily summary of all candidates (isin=None)
      'flip'      — notify when this ETF's abs signal flips (requires isin)
      'threshold' — notify when this ETF's dual_score crosses threshold (requires isin)
      'top_n'     — notify when this ETF enters top N of peer group (requires isin + top_n)
    """
    ensure_alert_tables(db_path)

    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO alert_subscriptions
               (email, isin, alert_type, threshold, top_n, lookback_months, active, created_at, notes)
               VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)""",
            (email, isin, alert_type, threshold, top_n, lookback_months,
             datetime.now().isoformat(timespec="seconds"), notes),
        )
        return cur.lastrowid


def remove_watch(
    email: str,
    isin: Optional[str] = None,
    alert_type: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """Deactivate matching subscriptions. Returns number of records deactivated."""
    ensure_alert_tables(db_path)
    clauses = ["email = ?"]
    params  = [email]
    if isin:       clauses.append("isin = ?");       params.append(isin)
    if alert_type: clauses.append("alert_type = ?"); params.append(alert_type)

    with get_conn(db_path) as conn:
        cur = conn.execute(
            f"UPDATE alert_subscriptions SET active=0 WHERE {' AND '.join(clauses)}",
            params,
        )
        return cur.rowcount


def list_watches(email: Optional[str] = None, db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    """List all active alert subscriptions."""
    ensure_alert_tables(db_path)
    sql = """
        SELECT s.id, s.email, s.isin, e.ticker, e.name, s.alert_type,
               s.threshold, s.top_n, s.lookback_months, s.created_at, s.last_triggered
        FROM alert_subscriptions s
        LEFT JOIN etfs e ON e.isin = s.isin
        WHERE s.active = 1
    """
    params = []
    if email:
        sql += " AND s.email = ?"
        params.append(email)
    sql += " ORDER BY s.created_at DESC"

    with get_conn(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_alert_log(n: int = 50, db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    """Fetch recent alert log entries."""
    ensure_alert_tables(db_path)
    with get_conn(db_path) as conn:
        return pd.read_sql_query(
            "SELECT * FROM alert_log ORDER BY sent_at DESC LIMIT ?",
            conn, params=(n,),
        )


# ─────────────────────────────────────────────────────────
# ALERT RUNNER
# ─────────────────────────────────────────────────────────

def run_digest(
    to: str,
    lookback_months: int = 12,
    threshold: float = 70,
    db_path: Path = DEFAULT_DB_PATH,
    config: Optional[SMTPConfig] = None,
) -> bool:
    """Fetch current candidates and send the daily digest."""
    ensure_alert_tables(db_path)

    df = get_candidates(lookback_months=lookback_months, min_dual_score=threshold, db_path=db_path)

    if df.empty:
        log.info("No candidates today — digest not sent (nothing to report).")
        return False

    # Get most recent risk-free rate
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT rate_pct FROM risk_free_rate ORDER BY rate_date DESC LIMIT 1"
        ).fetchone()
        rf = row[0] if row else 4.5

    snap_date = df["snap_date"].iloc[0] if "snap_date" in df.columns else date.today().isoformat()
    subject, html = build_digest_email(df, rf, snap_date, lookback_months)

    ok = send_email(to, subject, html, config)
    log_alert(to, "digest", subject, "sent" if ok else "failed", db_path=db_path)
    return ok


def run_flip_alerts(
    lookback_months: int = 12,
    lookback_days: int = 1,
    db_path: Path = DEFAULT_DB_PATH,
    config: Optional[SMTPConfig] = None,
) -> int:
    """
    Check for signal flips and notify all subscribed 'flip' watchers.
    Returns number of emails sent.
    """
    ensure_alert_tables(db_path)

    flips = get_signal_flips(lookback_days=lookback_days, lookback_months=lookback_months, db_path=db_path)
    if flips.empty:
        log.info(f"No signal flips in last {lookback_days} days.")
        return 0

    # Get all active 'flip' subscriptions
    with get_conn(db_path) as conn:
        subs = pd.read_sql_query(
            "SELECT * FROM alert_subscriptions WHERE active=1 AND alert_type IN ('flip','digest')",
            conn,
        )

    if subs.empty:
        log.info("Signal flips detected but no active subscriptions.")
        return 0

    sent = 0
    snap_date = date.today().isoformat()

    # Global flip subscribers (isin=None) get all flips
    global_subs = subs[subs["isin"].isna()]
    per_etf_subs = subs[subs["isin"].notna()]

    for _, sub in global_subs.iterrows():
        subject, html = build_flip_email(flips, snap_date)
        ok = send_email(sub["email"], subject, html, config)
        log_alert(sub["email"], "flip", subject, "sent" if ok else "failed", db_path=db_path)
        if ok: sent += 1

    # Per-ETF flip subscribers get alerts only for their watched ETF
    for _, sub in per_etf_subs.iterrows():
        etf_flips = flips[flips["isin"] == sub["isin"]]
        if etf_flips.empty:
            continue
        subject, html = build_flip_email(etf_flips, snap_date)
        ok = send_email(sub["email"], subject, html, config)
        log_alert(sub["email"], "flip", subject, "sent" if ok else "failed", sub["isin"], db_path=db_path)
        if ok: sent += 1

    log.info(f"Flip alerts sent: {sent}")
    return sent


def run_threshold_alerts(
    lookback_months: int = 12,
    db_path: Path = DEFAULT_DB_PATH,
    config: Optional[SMTPConfig] = None,
) -> int:
    """
    Check all 'threshold' subscriptions against current scores.
    Triggers if an ETF just crossed its threshold (comparing to yesterday's score).
    Returns number of emails sent.
    """
    ensure_alert_tables(db_path)

    with get_conn(db_path) as conn:
        subs = pd.read_sql_query(
            "SELECT * FROM alert_subscriptions WHERE active=1 AND alert_type='threshold' AND isin IS NOT NULL",
            conn,
        )
    if subs.empty:
        return 0

    df_latest = get_latest_scores(lookback_months=lookback_months, db_path=db_path)
    sent = 0

    for _, sub in subs.iterrows():
        isin = sub["isin"]
        threshold = sub["threshold"] or 70

        # Current score
        current = df_latest[df_latest["isin"] == isin]
        if current.empty:
            continue
        new_score = current.iloc[0]["dual_score"] or 0

        # Yesterday's score
        hist = get_score_history(isin, lookback_months=lookback_months, days=2, db_path=db_path)
        if len(hist) < 2:
            continue
        old_score = hist.iloc[-2]["dual_score"] or 0

        # Did it just cross the threshold?
        crossed_up   = old_score < threshold <= new_score
        crossed_down = new_score < threshold <= old_score

        if not (crossed_up or crossed_down):
            continue

        etf_row = current.iloc[0].to_dict()
        subject, html = build_threshold_email(etf_row, old_score, new_score)
        ok = send_email(sub["email"], subject, html, config)
        log_alert(sub["email"], "threshold", subject, "sent" if ok else "failed", isin, db_path=db_path)

        # Update last_triggered
        with get_conn(db_path) as conn:
            conn.execute(
                "UPDATE alert_subscriptions SET last_triggered=? WHERE id=?",
                (datetime.now().isoformat(timespec="seconds"), sub["id"]),
            )
        if ok: sent += 1

    log.info(f"Threshold alerts sent: {sent}")
    return sent


def run_all_alerts(
    lookback_months: int = 12,
    db_path: Path = DEFAULT_DB_PATH,
    config: Optional[SMTPConfig] = None,
) -> dict:
    """Run all alert types. Called by the daily pipeline runner."""
    results = {"flip": 0, "threshold": 0, "digest": 0}

    # 1. Signal flips (check last 1 day — nightly run)
    results["flip"] = run_flip_alerts(lookback_months=lookback_months, lookback_days=1, db_path=db_path, config=config)

    # 2. Threshold crossings
    results["threshold"] = run_threshold_alerts(lookback_months=lookback_months, db_path=db_path, config=config)

    # 3. Daily digest — send to all active 'digest' subscribers
    with get_conn(db_path) as conn:
        digest_subs = pd.read_sql_query(
            "SELECT DISTINCT email FROM alert_subscriptions WHERE active=1 AND alert_type='digest'",
            conn,
        )
    for _, row in digest_subs.iterrows():
        ok = run_digest(row["email"], lookback_months=lookback_months, db_path=db_path, config=config)
        if ok: results["digest"] += 1

    log.info(f"All alerts complete: {results}")
    return results


# ─────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="MomentumLens Alert Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send today's digest manually
  python alerts.py digest --to you@example.com

  # Add a daily digest subscription
  python alerts.py watch --email you@example.com --type digest

  # Watch SWDA for threshold crossing at 80
  python alerts.py watch --email you@example.com --isin IE00B4L5Y983 --type threshold --threshold 80

  # Watch SWDA for any signal flip
  python alerts.py watch --email you@example.com --isin IE00B4L5Y983 --type flip

  # List all subscriptions
  python alerts.py list

  # Remove all subscriptions for an email
  python alerts.py unwatch --email you@example.com

  # Run all alert checks (called by run_daily.py)
  python alerts.py check

  # Test SMTP config
  python alerts.py test --to you@example.com

  # View recent alert log
  python alerts.py log
        """
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    # digest
    p = sub.add_parser("digest", help="Send daily digest to an email address")
    p.add_argument("--to", required=True)
    p.add_argument("--lookback", type=int, default=12)
    p.add_argument("--threshold", type=float, default=70)

    # check
    p = sub.add_parser("check", help="Run all alert checks (flips + thresholds + digests)")
    p.add_argument("--lookback", type=int, default=12)

    # watch
    p = sub.add_parser("watch", help="Add an alert subscription")
    p.add_argument("--email", required=True)
    p.add_argument("--type", dest="alert_type", default="digest",
                   choices=["digest", "flip", "threshold", "top_n"])
    p.add_argument("--isin",      default=None)
    p.add_argument("--threshold", type=float, default=70)
    p.add_argument("--top-n",     type=int,   default=None)
    p.add_argument("--lookback",  type=int,   default=12)
    p.add_argument("--notes",     default=None)

    # unwatch
    p = sub.add_parser("unwatch", help="Remove alert subscriptions")
    p.add_argument("--email", required=True)
    p.add_argument("--isin",       default=None)
    p.add_argument("--type", dest="alert_type", default=None)

    # list
    p = sub.add_parser("list", help="List active subscriptions")
    p.add_argument("--email", default=None)

    # log
    p = sub.add_parser("log", help="Show recent alert log")
    p.add_argument("--n", type=int, default=20)

    # test
    p = sub.add_parser("test", help="Send a test email to verify SMTP config")
    p.add_argument("--to", required=True)

    args = parser.parse_args()
    db = DEFAULT_DB_PATH
    ensure_alert_tables(db)

    if args.cmd == "digest":
        ok = run_digest(args.to, args.lookback, args.threshold, db_path=db)
        print("✓ Digest sent" if ok else "✗ Digest failed or SMTP not configured")

    elif args.cmd == "check":
        results = run_all_alerts(args.lookback, db_path=db)
        print(f"Alerts sent — Flips: {results['flip']} | Thresholds: {results['threshold']} | Digests: {results['digest']}")

    elif args.cmd == "watch":
        sub_id = add_watch(
            email=args.email,
            alert_type=args.alert_type,
            isin=args.isin,
            threshold=args.threshold,
            top_n=args.top_n,
            lookback_months=args.lookback,
            notes=args.notes,
            db_path=db,
        )
        print(f"✓ Subscription added (id={sub_id}): {args.alert_type} for {args.email}"
              + (f" / {args.isin}" if args.isin else ""))

    elif args.cmd == "unwatch":
        n = remove_watch(args.email, args.isin, args.alert_type, db_path=db)
        print(f"✓ Deactivated {n} subscription(s) for {args.email}")

    elif args.cmd == "list":
        df = list_watches(args.email, db_path=db)
        if df.empty:
            print("No active subscriptions.")
        else:
            print(df[["id","email","ticker","alert_type","threshold","top_n","created_at","last_triggered"]].to_string(index=False))

    elif args.cmd == "log":
        df = get_alert_log(args.n, db_path=db)
        if df.empty:
            print("No alert log entries.")
        else:
            print(df[["sent_at","email","alert_type","status","subject"]].to_string(index=False))

    elif args.cmd == "test":
        cfg = SMTPConfig()
        if not cfg.is_configured:
            print("SMTP not configured. Set ML_SMTP_USER and ML_SMTP_PASS environment variables.")
            sys.exit(1)
        subject = f"MomentumLens — SMTP Test · {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        html = f"""<!DOCTYPE html><html><head><style>{EMAIL_CSS}</style></head><body>
        <div class="wrapper">
          <div class="header"><div class="logo">Momentum<span>Lens</span></div></div>
          <p style="color:#c8d8e8;font-size:14px;">
            ✓ SMTP configuration is working correctly.<br><br>
            Host: {cfg.host}:{cfg.port}<br>
            User: {cfg.user}
          </p>
        </div></body></html>"""
        ok = send_email(args.to, subject, html)
        print("✓ Test email sent successfully" if ok else "✗ Test email failed — check logs above")


if __name__ == "__main__":
    main()
