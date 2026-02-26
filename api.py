"""
MomentumLens — Phase 3: FastAPI Backend
========================================
Serves scored ETF data from the SQLite database built in Phase 2.

Endpoints:
    GET /                        Health check + summary
    GET /api/summary             DB stats and last run info
    GET /api/etfs                Paginated, filtered ETF list with scores
    GET /api/etfs/{isin}         Single ETF detail + score history
    GET /api/signals/latest      Current candidates above threshold
    GET /api/heatmap             Asset class × region aggregate grid
    GET /api/flips               Recent signal flips (abs momentum changes)
    GET /api/runs                Pipeline run history

Usage:
    pip install fastapi uvicorn
    uvicorn api:app --reload --port 8000

Then open:
    http://localhost:8000          → API health
    http://localhost:8000/docs     → Auto-generated Swagger UI
    http://localhost:8000/redoc    → ReDoc API reference
"""

import math
import sys
import logging
from datetime import date
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

# ── Internal modules ─────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from database import (
    get_latest_scores,
    get_score_history,
    get_candidates,
    get_heatmap_data,
    get_signal_flips,
    get_run_history,
    get_db_summary,
    get_conn,
    DEFAULT_DB_PATH,
)
from alerts import (
    ensure_alert_tables,
    add_watch,
    remove_watch,
    list_watches,
    get_alert_log,
    run_digest,
)

# ─────────────────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("momentum_lens.api")

app = FastAPI(
    title="MomentumLens API",
    description="LSE ETF Dual Momentum Scanner — data API",
    version="0.3.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow the dashboard HTML (served from disk or any origin in dev) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB = DEFAULT_DB_PATH


# ─────────────────────────────────────────────────────────
# RESPONSE MODELS
# ─────────────────────────────────────────────────────────

class ETFScore(BaseModel):
    isin: str
    ticker: Optional[str]
    name: Optional[str]
    asset_class: Optional[str]
    sub_asset: Optional[str]
    region: Optional[str]
    ter_pct: Optional[float]
    distribution: Optional[str]
    replication: Optional[str]
    aum_gbp_m: Optional[float]
    ret_1m: Optional[float]
    ret_3m: Optional[float]
    ret_6m: Optional[float]
    ret_12m: Optional[float]
    ret_ytd: Optional[float]
    snap_date: Optional[str]
    lookback_months: Optional[int]
    risk_free_rate: Optional[float]
    momentum_return: Optional[float]
    peer_group: Optional[str]
    rel_score: Optional[float]
    abs_signal: Optional[int]
    dual_score: Optional[float]
    signal_class: Optional[str]


class ScoreHistory(BaseModel):
    snap_date: str
    dual_score: Optional[float]
    rel_score: Optional[float]
    abs_signal: Optional[int]
    signal_class: Optional[str]
    ret_12m: Optional[float]
    ret_6m: Optional[float]
    ret_3m: Optional[float]
    ret_1m: Optional[float]
    aum_gbp_m: Optional[float]


class ETFDetail(BaseModel):
    etf: ETFScore
    history: List[ScoreHistory]


class HeatmapCell(BaseModel):
    asset_class: str
    region: Optional[str]
    etf_count: int
    avg_dual_score: Optional[float]
    avg_rel_score: Optional[float]
    abs_on_count: Optional[int]
    avg_momentum_return: Optional[float]


class SignalFlip(BaseModel):
    isin: str
    ticker: Optional[str]
    name: Optional[str]
    asset_class: Optional[str]
    flip_date: str
    prev_signal: int
    new_signal: int
    dual_score: Optional[float]


class RunRecord(BaseModel):
    id: int
    run_at: str
    snap_date: str
    status: str
    etfs_fetched: Optional[int]
    etfs_scored: Optional[int]
    candidates: Optional[int]
    risk_free_rate: Optional[float]
    lookback_months: Optional[int]
    duration_secs: Optional[float]
    error_message: Optional[str]


class PaginatedETFs(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    snap_date: Optional[str]
    data: List[ETFScore]


class Summary(BaseModel):
    active_etfs: Optional[int]
    daily_snapshots: Optional[int]
    first_snapshot: Optional[str]
    last_snapshot: Optional[str]
    total_score_rows: Optional[int]
    last_success_run: Optional[str]
    current_candidates: Optional[int]
    today: str


# ─────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────

def _clean(val):
    """Convert NaN floats to None for JSON serialisation."""
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def _row_to_etf(row: dict) -> ETFScore:
    return ETFScore(**{k: _clean(v) for k, v in row.items() if k in ETFScore.model_fields})


def _df_to_etf_list(df) -> List[ETFScore]:
    return [_row_to_etf(row) for row in df.to_dict(orient="records")]


# ─────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    """Health check — returns API status and quick summary."""
    try:
        summary = get_db_summary(DB)
        return {
            "status": "ok",
            "service": "MomentumLens API",
            "version": "0.3.0",
            "db_path": str(DB),
            "last_snapshot": summary.get("last_snapshot"),
            "active_etfs": summary.get("active_etfs"),
            "current_candidates": summary.get("current_candidates"),
        }
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


@app.get("/api/summary", response_model=Summary, tags=["Meta"])
def get_summary():
    """Database and pipeline summary statistics."""
    summary = get_db_summary(DB)
    return Summary(today=date.today().isoformat(), **summary)


@app.get("/api/etfs", response_model=PaginatedETFs, tags=["ETFs"])
def list_etfs(
    lookback: int        = Query(12,   description="Momentum lookback months (3, 6, or 12)"),
    page: int            = Query(1,    ge=1, description="Page number"),
    page_size: int       = Query(50,   ge=1, le=200, description="Results per page"),
    min_score: float     = Query(0,    ge=0, le=100, description="Minimum dual_score"),
    asset_class: str     = Query(None, description="Filter by asset class (e.g. Equity)"),
    min_aum: float       = Query(0,    ge=0, description="Minimum AUM in £M"),
    signal: str          = Query(None, description="Filter by signal_class: strong|moderate|weak|off"),
    sort: str            = Query("dual_score", description="Sort field"),
    order: str           = Query("desc", description="Sort order: asc|desc"),
):
    """
    Paginated list of all ETFs with their latest momentum scores.

    Use `min_score=70` to get candidates only. Use `signal=strong` to filter
    to Strong Candidates (dual_score ≥ 80).
    """
    # Map signal shorthand to signal_class values
    signal_map = {
        "strong":   "Strong Candidate",
        "moderate": "Moderate Candidate",
        "weak":     "Weak Signal",
        "off":      "No Signal (abs OFF)",
    }

    df = get_latest_scores(
        lookback_months=lookback,
        min_dual_score=min_score,
        asset_class=asset_class,
        min_aum_gbp_m=min_aum,
        db_path=DB,
    )

    if df.empty:
        return PaginatedETFs(total=0, page=page, page_size=page_size, pages=0, snap_date=None, data=[])

    # Signal filter (applied in Python after DB query)
    if signal and signal.lower() in signal_map:
        df = df[df["signal_class"] == signal_map[signal.lower()]]

    # Sort
    valid_sorts = {"dual_score", "rel_score", "ret_12m", "ret_6m", "aum_gbp_m", "ter_pct", "name"}
    sort_col = sort if sort in valid_sorts else "dual_score"
    ascending = order.lower() == "asc"
    if sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=ascending, na_position="last")

    # Pagination
    total = len(df)
    pages = max(1, math.ceil(total / page_size))
    offset = (page - 1) * page_size
    df_page = df.iloc[offset : offset + page_size]

    snap_date = df["snap_date"].iloc[0] if "snap_date" in df.columns and not df.empty else None

    return PaginatedETFs(
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
        snap_date=snap_date,
        data=_df_to_etf_list(df_page),
    )


@app.get("/api/etfs/{isin}", response_model=ETFDetail, tags=["ETFs"])
def get_etf(
    isin: str,
    lookback: int = Query(12, description="Lookback months for score history"),
    history_days: int = Query(90, ge=7, le=365, description="Days of score history to return"),
):
    """
    Full detail for a single ETF: latest scores + score history.
    Use history_days=365 to get a full year of trend data.
    """
    # Latest snapshot
    df_all = get_latest_scores(lookback_months=lookback, db_path=DB)
    df_etf = df_all[df_all["isin"] == isin.upper()]

    if df_etf.empty:
        raise HTTPException(status_code=404, detail=f"ETF not found: {isin}")

    etf_record = _row_to_etf(df_etf.iloc[0].to_dict())

    # Score history
    df_hist = get_score_history(isin.upper(), lookback_months=lookback, days=history_days, db_path=DB)

    history = []
    for row in df_hist.to_dict(orient="records"):
        history.append(ScoreHistory(**{k: _clean(v) for k, v in row.items() if k in ScoreHistory.model_fields}))

    return ETFDetail(etf=etf_record, history=history)


@app.get("/api/signals/latest", response_model=List[ETFScore], tags=["Signals"])
def latest_signals(
    lookback: int  = Query(12, description="Lookback months"),
    threshold: float = Query(70, ge=0, le=100, description="Min dual_score for candidates"),
    limit: int     = Query(50, ge=1, le=200),
):
    """
    Returns current dual momentum candidates above the threshold score,
    sorted by dual_score descending. This is the primary signal endpoint.
    """
    df = get_candidates(lookback_months=lookback, min_dual_score=threshold, db_path=DB)
    if df.empty:
        return []
    return _df_to_etf_list(df.head(limit))


@app.get("/api/heatmap", response_model=List[HeatmapCell], tags=["Analysis"])
def heatmap(
    lookback: int       = Query(12),
    snap_date: str      = Query(None, description="Specific date (YYYY-MM-DD). Defaults to latest."),
):
    """
    Aggregated dual momentum scores by asset class and region.
    Powers the heatmap view in the dashboard.
    """
    df = get_heatmap_data(snap_date=snap_date, lookback_months=lookback, db_path=DB)
    if df.empty:
        return []

    result = []
    for row in df.to_dict(orient="records"):
        result.append(HeatmapCell(
            asset_class=row.get("asset_class", ""),
            region=row.get("region"),
            etf_count=int(row.get("etf_count", 0)),
            avg_dual_score=_clean(row.get("avg_dual_score")),
            avg_rel_score=_clean(row.get("avg_rel_score")),
            abs_on_count=int(row.get("abs_on_count", 0)) if row.get("abs_on_count") is not None else None,
            avg_momentum_return=_clean(row.get("avg_momentum_return")),
        ))
    return result


@app.get("/api/flips", response_model=List[SignalFlip], tags=["Signals"])
def signal_flips(
    days: int    = Query(7, ge=1, le=90, description="Look back this many days for flips"),
    lookback: int = Query(12),
):
    """
    ETFs whose absolute momentum signal changed in the last N days.
    OFF→ON means the ETF just entered positive momentum territory.
    ON→OFF means it has dropped below the risk-free rate — a sell signal.
    """
    df = get_signal_flips(lookback_days=days, lookback_months=lookback, db_path=DB)
    if df.empty:
        return []

    result = []
    for row in df.to_dict(orient="records"):
        result.append(SignalFlip(
            isin=row["isin"],
            ticker=row.get("ticker"),
            name=row.get("name"),
            asset_class=row.get("asset_class"),
            flip_date=row["flip_date"],
            prev_signal=int(row["prev_signal"]),
            new_signal=int(row["new_signal"]),
            dual_score=_clean(row.get("dual_score")),
        ))
    return result


@app.get("/api/runs", response_model=List[RunRecord], tags=["Meta"])
def run_log(n: int = Query(30, ge=1, le=100)):
    """Pipeline run history. Shows the last N runs with timing and status."""
    df = get_run_history(n=n, db_path=DB)
    if df.empty:
        return []
    records = []
    for row in df.to_dict(orient="records"):
        records.append(RunRecord(**{k: _clean(v) for k, v in row.items() if k in RunRecord.model_fields}))
    return records


@app.get("/api/asset-classes", tags=["Meta"])
def asset_classes():
    """Distinct asset classes present in the database."""
    with get_conn(DB) as conn:
        rows = conn.execute("SELECT DISTINCT asset_class FROM etfs WHERE is_active=1 ORDER BY asset_class").fetchall()
    return [r[0] for r in rows if r[0]]


@app.get("/api/peer-groups", tags=["Meta"])
def peer_groups():
    """Distinct peer groups with ETF counts from latest scores."""
    with get_conn(DB) as conn:
        rows = conn.execute("""
            SELECT peer_group, COUNT(*) as count
            FROM daily_scores
            WHERE snap_date = (SELECT MAX(snap_date) FROM daily_scores)
            GROUP BY peer_group ORDER BY count DESC
        """).fetchall()
    return [{"peer_group": r[0], "count": r[1]} for r in rows]


# ─────────────────────────────────────────────────────────
# ALERT ENDPOINTS
# ─────────────────────────────────────────────────────────

class WatchRequest(BaseModel):
    email: str
    alert_type: str = "digest"   # digest | flip | threshold | top_n
    isin: Optional[str] = None
    threshold: Optional[float] = 70.0
    top_n: Optional[int] = None
    lookback_months: int = 12
    notes: Optional[str] = None


class WatchRecord(BaseModel):
    id: int
    email: str
    isin: Optional[str]
    ticker: Optional[str]
    name: Optional[str]
    alert_type: str
    threshold: Optional[float]
    top_n: Optional[int]
    lookback_months: int
    created_at: str
    last_triggered: Optional[str]


class AlertLogRecord(BaseModel):
    id: int
    sent_at: str
    email: str
    alert_type: str
    subject: Optional[str]
    isin: Optional[str]
    status: str
    error_message: Optional[str]


@app.post("/api/alerts/watch", response_model=dict, tags=["Alerts"])
def create_watch(req: WatchRequest):
    """
    Subscribe to an alert.

    alert_type options:
    - `digest`    — daily summary of all candidates (isin not required)
    - `flip`      — notify when abs momentum signal flips for a watched ETF (isin required)
    - `threshold` — notify when a watched ETF crosses a score threshold (isin + threshold required)
    - `top_n`     — notify when a watched ETF enters the top N of its peer group
    """
    ensure_alert_tables(DB)
    valid_types = {"digest", "flip", "threshold", "top_n"}
    if req.alert_type not in valid_types:
        raise HTTPException(400, f"alert_type must be one of: {valid_types}")
    if req.alert_type in {"flip", "threshold", "top_n"} and not req.isin:
        raise HTTPException(400, f"isin is required for alert_type='{req.alert_type}'")

    sub_id = add_watch(
        email=req.email,
        alert_type=req.alert_type,
        isin=req.isin.upper() if req.isin else None,
        threshold=req.threshold,
        top_n=req.top_n,
        lookback_months=req.lookback_months,
        notes=req.notes,
        db_path=DB,
    )
    return {"id": sub_id, "message": f"Subscription created (id={sub_id})"}


@app.delete("/api/alerts/watch", response_model=dict, tags=["Alerts"])
def delete_watch(
    email: str = Query(..., description="Email address to unsubscribe"),
    isin: str  = Query(None, description="ISIN to unwatch (omit to remove all for this email)"),
    alert_type: str = Query(None, description="Alert type to remove (omit for all types)"),
):
    """Remove alert subscriptions for an email address."""
    ensure_alert_tables(DB)
    n = remove_watch(email, isin, alert_type, db_path=DB)
    return {"deactivated": n, "message": f"Deactivated {n} subscription(s)"}


@app.get("/api/alerts/watch", response_model=List[WatchRecord], tags=["Alerts"])
def get_watches(email: str = Query(None, description="Filter by email (omit to see all)")):
    """List all active alert subscriptions."""
    ensure_alert_tables(DB)
    df = list_watches(email=email, db_path=DB)
    if df.empty:
        return []
    records = []
    for row in df.to_dict(orient="records"):
        records.append(WatchRecord(**{k: _clean(v) for k, v in row.items() if k in WatchRecord.model_fields}))
    return records


@app.get("/api/alerts/log", response_model=List[AlertLogRecord], tags=["Alerts"])
def alert_log(n: int = Query(50, ge=1, le=200)):
    """Recent alert send history."""
    ensure_alert_tables(DB)
    df = get_alert_log(n=n, db_path=DB)
    if df.empty:
        return []
    records = []
    for row in df.to_dict(orient="records"):
        records.append(AlertLogRecord(**{k: _clean(v) for k, v in row.items() if k in AlertLogRecord.model_fields}))
    return records


@app.post("/api/alerts/digest", response_model=dict, tags=["Alerts"])
def send_digest(
    email: str       = Query(..., description="Recipient email"),
    lookback: int    = Query(12),
    threshold: float = Query(70),
):
    """
    Manually trigger a digest email to an address.
    Requires SMTP to be configured via environment variables.
    """
    ok = run_digest(email, lookback_months=lookback, threshold=threshold, db_path=DB)
    if ok:
        return {"status": "sent", "message": f"Digest sent to {email}"}
    return {"status": "failed", "message": "SMTP not configured or no candidates to report"}


# ─────────────────────────────────────────────────────────
# SERVE DASHBOARD (convenience — avoids CORS in dev)
# ─────────────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def serve_dashboard():
    """Serve the dashboard HTML from the same directory."""
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h2>dashboard.html not found in same directory as api.py</h2>", status_code=404)


# ─────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
