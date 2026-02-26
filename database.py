"""
MomentumLens — Phase 2: Database Layer
=======================================
Manages the SQLite database for persisting daily ETF snapshots,
momentum scores, and run metadata.

Schema:
    etfs              — master ETF reference table (updated on each run)
    daily_performance — one row per ETF per day (raw returns from JustETF)
    daily_scores      — one row per ETF per day (computed momentum scores)
    run_log           — one row per pipeline run (audit trail)
    risk_free_rate    — daily risk-free rate history

This module is imported by both the daily runner and (later) the web API.
It has zero dependencies beyond the Python standard library + pandas.
"""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger("momentum_lens.db")

# ─────────────────────────────────────────────────────────
# DATABASE PATH
# ─────────────────────────────────────────────────────────

DEFAULT_DB_PATH = Path(__file__).parent / "momentum_lens.db"


# ─────────────────────────────────────────────────────────
# CONNECTION MANAGER
# ─────────────────────────────────────────────────────────

@contextmanager
def get_conn(db_path: Path = DEFAULT_DB_PATH):
    """Context manager for SQLite connections with WAL mode and FK support."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL") # balance safety vs speed
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────
# SCHEMA CREATION
# ─────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- ── ETF master reference ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etfs (
    isin            TEXT PRIMARY KEY,
    ticker          TEXT,
    name            TEXT,
    asset_class     TEXT,
    sub_asset       TEXT,
    region          TEXT,
    index_name      TEXT,
    distribution    TEXT,   -- Accumulating / Distributing
    replication     TEXT,   -- Physical / Synthetic
    ter_pct         REAL,
    inception_date  TEXT,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    is_active       INTEGER NOT NULL DEFAULT 1   -- 0 if delisted
);

CREATE INDEX IF NOT EXISTS idx_etfs_asset_class ON etfs(asset_class);
CREATE INDEX IF NOT EXISTS idx_etfs_ticker       ON etfs(ticker);

-- ── Daily raw performance snapshots ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_performance (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    isin        TEXT NOT NULL REFERENCES etfs(isin),
    snap_date   TEXT NOT NULL,   -- ISO date: YYYY-MM-DD
    aum_gbp_m   REAL,
    ret_1m      REAL,
    ret_3m      REAL,
    ret_6m      REAL,
    ret_12m     REAL,
    ret_ytd     REAL,
    ret_3y      REAL,
    UNIQUE(isin, snap_date)
);

CREATE INDEX IF NOT EXISTS idx_perf_isin_date ON daily_performance(isin, snap_date);
CREATE INDEX IF NOT EXISTS idx_perf_date      ON daily_performance(snap_date);

-- ── Daily computed momentum scores ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_scores (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    isin             TEXT NOT NULL REFERENCES etfs(isin),
    snap_date        TEXT NOT NULL,
    lookback_months  INTEGER NOT NULL DEFAULT 12,
    risk_free_rate   REAL NOT NULL,
    momentum_return  REAL,    -- primary return minus 1M (skip-month)
    peer_group       TEXT,
    rel_score        REAL,    -- 0-100 percentile within peer group
    abs_signal       INTEGER, -- 1 = ON, 0 = OFF
    dual_score       REAL,    -- 0-100 combined score
    signal_class     TEXT,    -- Strong / Moderate / Weak / No Signal
    UNIQUE(isin, snap_date, lookback_months)
);

CREATE INDEX IF NOT EXISTS idx_scores_isin_date ON daily_scores(isin, snap_date);
CREATE INDEX IF NOT EXISTS idx_scores_date       ON daily_scores(snap_date);
CREATE INDEX IF NOT EXISTS idx_scores_dual       ON daily_scores(dual_score DESC);

-- ── Pipeline run audit log ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT NOT NULL,          -- ISO datetime
    snap_date       TEXT NOT NULL,          -- date data applies to
    status          TEXT NOT NULL,          -- success / failed / partial
    etfs_fetched    INTEGER,
    etfs_scored     INTEGER,
    candidates      INTEGER,
    risk_free_rate  REAL,
    lookback_months INTEGER,
    duration_secs   REAL,
    error_message   TEXT
);

-- ── Risk-free rate history ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS risk_free_rate (
    rate_date   TEXT PRIMARY KEY,   -- ISO date
    rate_pct    REAL NOT NULL,
    source      TEXT                -- e.g. "FRED:GBATB3M"
);
"""


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Create database and all tables if they don't already exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
    log.info(f"Database initialised: {db_path}")


# ─────────────────────────────────────────────────────────
# WRITE OPERATIONS
# ─────────────────────────────────────────────────────────

def upsert_etfs(df: pd.DataFrame, db_path: Path = DEFAULT_DB_PATH) -> int:
    """
    Insert or update ETF reference data.
    Updates last_seen on every run; preserves first_seen.
    Returns number of rows upserted.
    """
    today = date.today().isoformat()

    records = []
    for _, row in df.iterrows():
        records.append({
            "isin":           row["isin"],
            "ticker":         row.get("ticker", ""),
            "name":           row.get("name", ""),
            "asset_class":    row.get("asset_class", ""),
            "sub_asset":      row.get("sub_asset", ""),
            "region":         row.get("region", ""),
            "index_name":     row.get("index_name", ""),
            "distribution":   row.get("distribution", ""),
            "replication":    row.get("replication", ""),
            "ter_pct":        _to_float(row.get("ter_pct")),
            "inception_date": row.get("inception_date", ""),
            "first_seen":     today,
            "last_seen":      today,
            "is_active":      1,
        })

    sql = """
        INSERT INTO etfs
            (isin, ticker, name, asset_class, sub_asset, region, index_name,
             distribution, replication, ter_pct, inception_date, first_seen, last_seen, is_active)
        VALUES
            (:isin, :ticker, :name, :asset_class, :sub_asset, :region, :index_name,
             :distribution, :replication, :ter_pct, :inception_date, :first_seen, :last_seen, :is_active)
        ON CONFLICT(isin) DO UPDATE SET
            ticker         = excluded.ticker,
            name           = excluded.name,
            asset_class    = excluded.asset_class,
            sub_asset      = excluded.sub_asset,
            region         = excluded.region,
            index_name     = excluded.index_name,
            distribution   = excluded.distribution,
            replication    = excluded.replication,
            ter_pct        = excluded.ter_pct,
            last_seen      = excluded.last_seen,
            is_active      = 1
    """

    with get_conn(db_path) as conn:
        conn.executemany(sql, records)

    log.info(f"Upserted {len(records)} ETF reference records")
    return len(records)


def insert_daily_performance(
    df: pd.DataFrame,
    snap_date: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """
    Insert daily performance snapshot. Skips rows already in DB for this date
    (idempotent — safe to re-run for the same day).
    Returns number of new rows inserted.
    """
    records = []
    for _, row in df.iterrows():
        records.append({
            "isin":      row["isin"],
            "snap_date": snap_date,
            "aum_gbp_m": _to_float(row.get("aum_gbp_m")),
            "ret_1m":    _to_float(row.get("ret_1m")),
            "ret_3m":    _to_float(row.get("ret_3m")),
            "ret_6m":    _to_float(row.get("ret_6m")),
            "ret_12m":   _to_float(row.get("ret_12m")),
            "ret_ytd":   _to_float(row.get("ret_ytd")),
            "ret_3y":    _to_float(row.get("ret_3y")),
        })

    sql = """
        INSERT OR IGNORE INTO daily_performance
            (isin, snap_date, aum_gbp_m, ret_1m, ret_3m, ret_6m, ret_12m, ret_ytd, ret_3y)
        VALUES
            (:isin, :snap_date, :aum_gbp_m, :ret_1m, :ret_3m, :ret_6m, :ret_12m, :ret_ytd, :ret_3y)
    """

    with get_conn(db_path) as conn:
        conn.executemany(sql, records)

    log.info(f"Inserted daily_performance for {snap_date}: {len(records)} rows")
    return len(records)


def insert_daily_scores(
    df: pd.DataFrame,
    snap_date: str,
    risk_free_rate: float,
    lookback_months: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """
    Insert computed momentum scores. Idempotent via INSERT OR REPLACE.
    Returns number of rows written.
    """
    records = []
    for _, row in df.iterrows():
        records.append({
            "isin":            row["isin"],
            "snap_date":       snap_date,
            "lookback_months": lookback_months,
            "risk_free_rate":  risk_free_rate,
            "momentum_return": _to_float(row.get("momentum_return")),
            "peer_group":      row.get("peer_group", ""),
            "rel_score":       _to_float(row.get("rel_score")),
            "abs_signal":      int(row.get("abs_signal", 0)),
            "dual_score":      _to_float(row.get("dual_score")),
            "signal_class":    row.get("signal_class", ""),
        })

    sql = """
        INSERT OR REPLACE INTO daily_scores
            (isin, snap_date, lookback_months, risk_free_rate, momentum_return,
             peer_group, rel_score, abs_signal, dual_score, signal_class)
        VALUES
            (:isin, :snap_date, :lookback_months, :risk_free_rate, :momentum_return,
             :peer_group, :rel_score, :abs_signal, :dual_score, :signal_class)
    """

    with get_conn(db_path) as conn:
        conn.executemany(sql, records)

    log.info(f"Inserted daily_scores for {snap_date}: {len(records)} rows (lookback={lookback_months}M)")
    return len(records)


def insert_risk_free_rate(
    rate_date: str,
    rate_pct: float,
    source: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Store the risk-free rate used for a given date."""
    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO risk_free_rate (rate_date, rate_pct, source) VALUES (?, ?, ?)",
            (rate_date, rate_pct, source),
        )


def log_run(
    snap_date: str,
    status: str,
    etfs_fetched: int = 0,
    etfs_scored: int = 0,
    candidates: int = 0,
    risk_free_rate: float = 0.0,
    lookback_months: int = 12,
    duration_secs: float = 0.0,
    error_message: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Write a run audit record."""
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT INTO run_log
                (run_at, snap_date, status, etfs_fetched, etfs_scored, candidates,
                 risk_free_rate, lookback_months, duration_secs, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(timespec="seconds"),
                snap_date,
                status,
                etfs_fetched,
                etfs_scored,
                candidates,
                risk_free_rate,
                lookback_months,
                round(duration_secs, 1),
                error_message,
            ),
        )
    log.info(f"Run logged: {status} | {etfs_scored} scored | {candidates} candidates")


# ─────────────────────────────────────────────────────────
# READ OPERATIONS
# ─────────────────────────────────────────────────────────

def get_latest_scores(
    lookback_months: int = 12,
    min_dual_score: float = 0,
    asset_class: Optional[str] = None,
    min_aum_gbp_m: float = 0,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    Return the most recent scored snapshot, joined with ETF metadata.

    Parameters
    ----------
    lookback_months : filter by lookback window used
    min_dual_score  : only return ETFs with dual_score ≥ this value
    asset_class     : filter by asset class (e.g. 'Equity')
    min_aum_gbp_m   : minimum AUM filter, applied from last performance record

    Returns pd.DataFrame sorted by dual_score descending.
    """
    latest_date_sql = """
        SELECT MAX(snap_date) FROM daily_scores WHERE lookback_months = ?
    """

    where_clauses = ["s.lookback_months = ?", "s.snap_date = ?", "s.dual_score >= ?"]
    params = [lookback_months]

    with get_conn(db_path) as conn:
        row = conn.execute(latest_date_sql, (lookback_months,)).fetchone()
        latest_date = row[0] if row and row[0] else None

    if not latest_date:
        log.warning("No scores found in database.")
        return pd.DataFrame()

    params.append(latest_date)
    params.append(min_dual_score)

    if asset_class:
        where_clauses.append("e.asset_class = ?")
        params.append(asset_class)

    if min_aum_gbp_m > 0:
        where_clauses.append("p.aum_gbp_m >= ?")
        params.append(min_aum_gbp_m)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            e.isin, e.ticker, e.name, e.asset_class, e.sub_asset, e.region,
            e.ter_pct, e.distribution, e.replication,
            p.aum_gbp_m, p.ret_1m, p.ret_3m, p.ret_6m, p.ret_12m, p.ret_ytd,
            s.snap_date, s.lookback_months, s.risk_free_rate,
            s.momentum_return, s.peer_group,
            s.rel_score, s.abs_signal, s.dual_score, s.signal_class
        FROM daily_scores s
        JOIN etfs e ON e.isin = s.isin
        LEFT JOIN daily_performance p ON p.isin = s.isin AND p.snap_date = s.snap_date
        WHERE {where_sql}
        ORDER BY s.dual_score DESC
    """

    with get_conn(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    log.info(f"Loaded {len(df)} ETFs from DB (date={latest_date}, lookback={lookback_months}M)")
    return df


def get_score_history(
    isin: str,
    lookback_months: int = 12,
    days: int = 90,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    Return the score history for a single ETF over the last N days.
    Used for trend charts and signal-flip detection.
    """
    sql = """
        SELECT
            s.snap_date, s.dual_score, s.rel_score, s.abs_signal, s.signal_class,
            p.ret_12m, p.ret_6m, p.ret_3m, p.ret_1m, p.aum_gbp_m
        FROM daily_scores s
        LEFT JOIN daily_performance p ON p.isin = s.isin AND p.snap_date = s.snap_date
        WHERE s.isin = ?
          AND s.lookback_months = ?
          AND s.snap_date >= date('now', ?)
        ORDER BY s.snap_date ASC
    """
    days_param = f"-{days} days"

    with get_conn(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=(isin, lookback_months, days_param))

    return df


def get_candidates(
    lookback_months: int = 12,
    min_dual_score: float = 70,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    Convenience wrapper: return only current candidates (dual_score ≥ threshold).
    """
    return get_latest_scores(
        lookback_months=lookback_months,
        min_dual_score=min_dual_score,
        db_path=db_path,
    )


def get_heatmap_data(
    snap_date: Optional[str] = None,
    lookback_months: int = 12,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    Return aggregated data for the asset class heatmap.
    One row per (asset_class, region) pair with median dual_score
    and count of ETFs with positive absolute momentum.
    """
    if snap_date is None:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT MAX(snap_date) FROM daily_scores WHERE lookback_months = ?",
                (lookback_months,),
            ).fetchone()
            snap_date = row[0] if row and row[0] else date.today().isoformat()

    sql = """
        SELECT
            e.asset_class,
            e.region,
            COUNT(*) as etf_count,
            ROUND(AVG(s.dual_score), 1) as avg_dual_score,
            ROUND(AVG(s.rel_score), 1) as avg_rel_score,
            SUM(s.abs_signal) as abs_on_count,
            ROUND(AVG(s.momentum_return), 2) as avg_momentum_return
        FROM daily_scores s
        JOIN etfs e ON e.isin = s.isin
        WHERE s.snap_date = ? AND s.lookback_months = ?
        GROUP BY e.asset_class, e.region
        ORDER BY avg_dual_score DESC
    """

    with get_conn(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=(snap_date, lookback_months))

    return df


def get_signal_flips(
    lookback_days: int = 7,
    lookback_months: int = 12,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    Detect ETFs whose absolute momentum signal flipped in the last N days.
    Returns rows with: isin, ticker, name, flip_date, old_signal, new_signal.
    Useful for alert generation.
    """
    sql = """
        WITH ranked AS (
            SELECT
                s.isin,
                s.snap_date,
                s.abs_signal,
                s.dual_score,
                e.ticker,
                e.name,
                e.asset_class,
                LAG(s.abs_signal) OVER (PARTITION BY s.isin ORDER BY s.snap_date) as prev_signal,
                LAG(s.snap_date)  OVER (PARTITION BY s.isin ORDER BY s.snap_date) as prev_date
            FROM daily_scores s
            JOIN etfs e ON e.isin = s.isin
            WHERE s.lookback_months = ?
              AND s.snap_date >= date('now', ?)
        )
        SELECT isin, ticker, name, asset_class,
               snap_date as flip_date, prev_signal, abs_signal as new_signal, dual_score
        FROM ranked
        WHERE prev_signal IS NOT NULL
          AND prev_signal != abs_signal
        ORDER BY flip_date DESC
    """

    days_param = f"-{lookback_days} days"

    with get_conn(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=(lookback_months, days_param))

    return df


def get_run_history(n: int = 30, db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    """Return the last N pipeline run records."""
    sql = """
        SELECT * FROM run_log
        ORDER BY run_at DESC
        LIMIT ?
    """
    with get_conn(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=(n,))


def get_db_summary(db_path: Path = DEFAULT_DB_PATH) -> dict:
    """Return a summary dict of database contents."""
    with get_conn(db_path) as conn:
        etf_count     = conn.execute("SELECT COUNT(*) FROM etfs WHERE is_active=1").fetchone()[0]
        snap_count    = conn.execute("SELECT COUNT(DISTINCT snap_date) FROM daily_performance").fetchone()[0]
        first_snap    = conn.execute("SELECT MIN(snap_date) FROM daily_performance").fetchone()[0]
        last_snap     = conn.execute("SELECT MAX(snap_date) FROM daily_performance").fetchone()[0]
        score_count   = conn.execute("SELECT COUNT(*) FROM daily_scores").fetchone()[0]
        last_run      = conn.execute("SELECT MAX(run_at) FROM run_log WHERE status='success'").fetchone()[0]
        candidate_cnt = conn.execute(
            "SELECT COUNT(*) FROM daily_scores WHERE snap_date=(SELECT MAX(snap_date) FROM daily_scores) AND dual_score>=70"
        ).fetchone()[0]

    return {
        "active_etfs":      etf_count,
        "daily_snapshots":  snap_count,
        "first_snapshot":   first_snap,
        "last_snapshot":    last_snap,
        "total_score_rows": score_count,
        "last_success_run": last_run,
        "current_candidates": candidate_cnt,
    }


# ─────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────

def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        import math
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────
# CLI — quick inspection tool
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="MomentumLens DB inspector")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("init",    help="Initialise database schema")
    sub.add_parser("summary", help="Print DB summary stats")
    sub.add_parser("runs",    help="Show recent pipeline runs")

    show_p = sub.add_parser("candidates", help="Show current candidates")
    show_p.add_argument("--lookback", type=int, default=12)
    show_p.add_argument("--threshold", type=float, default=70)

    hist_p = sub.add_parser("history", help="Score history for one ETF")
    hist_p.add_argument("isin")
    hist_p.add_argument("--days", type=int, default=90)

    flip_p = sub.add_parser("flips", help="Recent signal flips")
    flip_p.add_argument("--days", type=int, default=14)

    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    if args.cmd == "init":
        init_db(args.db)
        print(f"Database initialised at {args.db}")

    elif args.cmd == "summary":
        summary = get_db_summary(args.db)
        print("\n  MomentumLens Database Summary")
        print("  " + "─" * 40)
        for k, v in summary.items():
            print(f"  {k:<22} {v}")
        print()

    elif args.cmd == "runs":
        df = get_run_history(db_path=args.db)
        if df.empty:
            print("No runs recorded yet.")
        else:
            print(df[["run_at","snap_date","status","etfs_scored","candidates","duration_secs"]].to_string(index=False))

    elif args.cmd == "candidates":
        df = get_candidates(args.lookback, args.threshold, db_path=args.db)
        if df.empty:
            print("No candidates found.")
        else:
            cols = ["ticker","name","asset_class","dual_score","rel_score","abs_signal","signal_class"]
            print(df[[c for c in cols if c in df.columns]].to_string(index=False))

    elif args.cmd == "history":
        df = get_score_history(args.isin, days=args.days, db_path=args.db)
        if df.empty:
            print(f"No history found for {args.isin}")
        else:
            print(df.to_string(index=False))

    elif args.cmd == "flips":
        df = get_signal_flips(args.days, db_path=args.db)
        if df.empty:
            print(f"No signal flips in last {args.days} days.")
        else:
            print(df.to_string(index=False))

    else:
        parser.print_help()
