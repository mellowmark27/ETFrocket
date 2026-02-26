"""
MomentumLens — Phase 2: Daily Pipeline Runner
==============================================
Orchestrates the full end-to-end pipeline:
    1. Fetch ETFs from JustETF
    2. Fetch risk-free rate from FRED
    3. Score with dual momentum engine
    4. Persist to SQLite database
    5. Export daily CSV snapshot (optional)
    6. Print summary + signal flip alerts

Designed to be run daily after LSE close (typically 4:30–5pm UK time).

Usage:
    python run_daily.py                       # standard daily run
    python run_daily.py --lookback 6          # alternative lookback
    python run_daily.py --dry-run             # fetch + score but don't write to DB
    python run_daily.py --export              # also export a dated CSV
    python run_daily.py --backfill 2024-01-01 # re-score using data already in DB
    python run_daily.py --status              # show DB summary and exit
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# ── Internal modules ────────────────────────────────────
# Ensure the package directory is on path when running directly
sys.path.insert(0, str(Path(__file__).parent))

from momentum_scanner import (
    fetch_lse_etfs,
    fetch_risk_free_rate,
    compute_dual_momentum,
    save_results,
    print_summary,
    CANDIDATE_THRESHOLD,
)
from database import (
    init_db,
    upsert_etfs,
    insert_daily_performance,
    insert_daily_scores,
    insert_risk_free_rate,
    log_run,
    get_signal_flips,
    get_db_summary,
    get_candidates,
    DEFAULT_DB_PATH,
)
try:
    from alerts import run_all_alerts, ensure_alert_tables
    ALERTS_AVAILABLE = True
except ImportError:
    ALERTS_AVAILABLE = False

# ─────────────────────────────────────────────────────────
# LOGGING — write to both console and rotating log file
# ─────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"

def setup_logging(verbose: bool = False):
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"run_{date.today().isoformat()}.log"

    level = logging.DEBUG if verbose else logging.INFO
    fmt   = "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s"

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger("momentum_lens.runner")


# ─────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────

def run_pipeline(
    lookback_months: int = 12,
    min_aum_gbp: float = 0,
    rf_rate_override: float = None,
    dry_run: bool = False,
    export_csv: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    verbose: bool = False,
) -> dict:
    """
    Run the full daily pipeline. Returns a result dict with run stats.

    Parameters
    ----------
    lookback_months   : momentum lookback (12, 6, or 3)
    min_aum_gbp       : minimum AUM filter in £M
    rf_rate_override  : manual risk-free rate override in %
    dry_run           : if True, skip all database writes
    export_csv        : if True, write dated CSV to exports/ directory
    db_path           : path to SQLite database
    verbose           : enable debug logging
    """
    log = setup_logging(verbose)
    snap_date = date.today().isoformat()
    t_start = time.time()

    log.info("=" * 60)
    log.info(f"MomentumLens Daily Pipeline — {snap_date}")
    log.info(f"Lookback: {lookback_months}-1M | Min AUM: £{min_aum_gbp}M | Dry run: {dry_run}")
    log.info("=" * 60)

    result = {
        "snap_date":       snap_date,
        "status":          "failed",
        "etfs_fetched":    0,
        "etfs_scored":     0,
        "candidates":      0,
        "risk_free_rate":  0.0,
        "duration_secs":   0.0,
        "error":           None,
    }

    try:
        # ── Step 1: Initialise DB ──────────────────────────────────────
        if not dry_run:
            init_db(db_path)

        # ── Step 2: Fetch ETFs from JustETF ───────────────────────────
        log.info("Step 1/4: Fetching ETFs from JustETF...")
        df_etfs = fetch_lse_etfs(min_aum_gbp=min_aum_gbp)
        result["etfs_fetched"] = len(df_etfs)
        log.info(f"  ✓ {len(df_etfs)} ETFs fetched")

        # ── Step 3: Risk-free rate ─────────────────────────────────────
        log.info("Step 2/4: Getting risk-free rate...")
        if rf_rate_override is not None:
            rf_rate = rf_rate_override
            rf_source = "manual_override"
            log.info(f"  Using manual override: {rf_rate:.2f}%")
        else:
            rf_rate = fetch_risk_free_rate()
            rf_source = "FRED"
        result["risk_free_rate"] = rf_rate

        # ── Step 4: Score ──────────────────────────────────────────────
        log.info("Step 3/4: Computing dual momentum scores...")
        df_scored = compute_dual_momentum(
            df_etfs,
            lookback_months=lookback_months,
            risk_free_rate=rf_rate,
        )
        result["etfs_scored"] = len(df_scored)

        n_candidates = int((df_scored["dual_score"] >= CANDIDATE_THRESHOLD).sum())
        result["candidates"] = n_candidates
        result["status"] = "success"

        # ── Step 5: Persist to DB ──────────────────────────────────────
        if not dry_run:
            log.info("Step 4/4: Persisting to database...")
            upsert_etfs(df_etfs, db_path=db_path)
            insert_daily_performance(df_etfs, snap_date, db_path=db_path)
            insert_daily_scores(df_scored, snap_date, rf_rate, lookback_months, db_path=db_path)
            insert_risk_free_rate(snap_date, rf_rate, rf_source, db_path=db_path)
            log.info("  ✓ Database updated")
        else:
            log.info("  [DRY RUN] Skipping database writes")

        # ── Step 6: Export CSV (optional) ─────────────────────────────
        if export_csv:
            export_dir = Path(__file__).parent / "exports"
            export_dir.mkdir(exist_ok=True)
            csv_path = export_dir / f"momentum_scores_{snap_date}.csv"
            save_results(df_scored, str(csv_path), rf_rate, lookback_months)
            log.info(f"  ✓ CSV exported: {csv_path}")

        # ── Step 7: Print summary ──────────────────────────────────────
        print_summary(df_scored, top_n=20)

        # ── Step 8: Alerts (signal flips + threshold + digest) ────────
        if not dry_run and ALERTS_AVAILABLE:
            ensure_alert_tables(db_path)
            alert_results = run_all_alerts(lookback_months=lookback_months, db_path=db_path)
            log.info(
                f"Alerts — flips: {alert_results['flip']} | "
                f"threshold: {alert_results['threshold']} | "
                f"digest: {alert_results['digest']}"
            )
        elif not dry_run:
            # Fallback: just log flips to console
            _report_signal_flips(db_path, lookback_months, log)

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        log.error(f"Pipeline failed: {e}", exc_info=True)

    finally:
        result["duration_secs"] = round(time.time() - t_start, 1)

        if not dry_run:
            log_run(
                snap_date=snap_date,
                status=result["status"],
                etfs_fetched=result["etfs_fetched"],
                etfs_scored=result["etfs_scored"],
                candidates=result["candidates"],
                risk_free_rate=result["risk_free_rate"],
                lookback_months=lookback_months,
                duration_secs=result["duration_secs"],
                error_message=result.get("error"),
                db_path=db_path,
            )

        log.info(
            f"Pipeline complete — status={result['status']} | "
            f"{result['etfs_scored']} scored | {result['candidates']} candidates | "
            f"{result['duration_secs']}s"
        )

    return result


def _report_signal_flips(db_path: Path, lookback_months: int, log) -> None:
    """Check for signal flips in the last 3 days and log them."""
    flips = get_signal_flips(lookback_days=3, lookback_months=lookback_months, db_path=db_path)

    if flips.empty:
        log.info("Signal flips (last 3 days): none")
        return

    log.info(f"⚡ Signal flips detected ({len(flips)}):")
    for _, row in flips.iterrows():
        direction = "OFF → ON  🟢" if row["new_signal"] == 1 else "ON  → OFF 🔴"
        log.info(
            f"   {direction}  {row['ticker']:<8} {row['name'][:40]:<40} "
            f"dual_score={row['dual_score']:.1f}  ({row['flip_date']})"
        )


# ─────────────────────────────────────────────────────────
# BACKFILL — re-score historical data already in DB
# ─────────────────────────────────────────────────────────

def backfill_scores(
    from_date: str,
    lookback_months: int = 12,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """
    Re-compute and re-store momentum scores for all snapshots from from_date onwards.

    Useful when:
    - The scoring algorithm is updated and you want to re-score history
    - A new lookback window is added
    - Peer group definitions change

    Data must already exist in daily_performance (i.e. you can't fetch history
    from JustETF retroactively — this only works on data already stored).
    """
    log = logging.getLogger("momentum_lens.runner")

    from database import get_conn

    # Get all distinct dates >= from_date that have performance data
    sql = """
        SELECT DISTINCT snap_date FROM daily_performance
        WHERE snap_date >= ?
        ORDER BY snap_date ASC
    """
    with get_conn(db_path) as conn:
        dates = [row[0] for row in conn.execute(sql, (from_date,)).fetchall()]

    if not dates:
        log.warning(f"No performance data found from {from_date} onwards.")
        return

    log.info(f"Backfilling scores for {len(dates)} dates from {from_date}...")

    for snap_date in dates:
        log.info(f"  Backfilling {snap_date}...")

        # Load performance data for this date
        perf_sql = """
            SELECT e.isin, e.ticker, e.name, e.asset_class, e.sub_asset, e.region,
                   e.ter_pct, e.distribution, e.replication,
                   p.aum_gbp_m, p.ret_1m, p.ret_3m, p.ret_6m, p.ret_12m, p.ret_ytd, p.ret_3y
            FROM daily_performance p
            JOIN etfs e ON e.isin = p.isin
            WHERE p.snap_date = ?
        """
        with get_conn(db_path) as conn:
            df = pd.read_sql_query(perf_sql, conn, params=(snap_date,))

        if df.empty:
            log.warning(f"    No data for {snap_date}, skipping")
            continue

        # Get risk-free rate for this date (fall back to nearest available)
        rf_sql = """
            SELECT rate_pct FROM risk_free_rate
            WHERE rate_date <= ? ORDER BY rate_date DESC LIMIT 1
        """
        with get_conn(db_path) as conn:
            rf_row = conn.execute(rf_sql, (snap_date,)).fetchone()
            rf_rate = rf_row[0] if rf_row else 4.5

        # Score
        df_scored = compute_dual_momentum(df, lookback_months=lookback_months, risk_free_rate=rf_rate)

        # Store
        insert_daily_scores(df_scored, snap_date, rf_rate, lookback_months, db_path=db_path)

    log.info(f"Backfill complete for {len(dates)} dates.")


# ─────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="MomentumLens — Daily Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_daily.py                           # standard daily run
  python run_daily.py --dry-run                 # test without DB writes
  python run_daily.py --export                  # also export CSV
  python run_daily.py --lookback 6              # 6-month lookback
  python run_daily.py --min-aum 200             # filter thin ETFs
  python run_daily.py --rf-rate 4.75            # override risk-free rate
  python run_daily.py --status                  # show DB summary and exit
  python run_daily.py --backfill 2024-06-01     # re-score from date
  python run_daily.py --verbose                 # debug logging
        """
    )
    parser.add_argument("--lookback",   type=int,   choices=[3,6,12], default=12)
    parser.add_argument("--min-aum",    type=float, default=0, metavar="GBP_M")
    parser.add_argument("--rf-rate",    type=float, default=None, metavar="PCT")
    parser.add_argument("--dry-run",    action="store_true", help="Don't write to database")
    parser.add_argument("--export",     action="store_true", help="Export dated CSV to exports/")
    parser.add_argument("--verbose",    action="store_true", help="Debug logging")
    parser.add_argument("--status",     action="store_true", help="Print DB summary and exit")
    parser.add_argument("--db",         type=Path, default=DEFAULT_DB_PATH, metavar="PATH")
    parser.add_argument(
        "--backfill", type=str, default=None, metavar="YYYY-MM-DD",
        help="Re-score all stored data from this date (requires existing DB data)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.status:
        logging.basicConfig(level=logging.WARNING)
        summary = get_db_summary(args.db)
        print("\n  MomentumLens — Database Status")
        print("  " + "─" * 42)
        for k, v in summary.items():
            print(f"  {k:<24} {v}")
        print()
        return

    if args.backfill:
        setup_logging(args.verbose)
        backfill_scores(args.backfill, args.lookback, db_path=args.db)
        return

    result = run_pipeline(
        lookback_months=args.lookback,
        min_aum_gbp=args.min_aum,
        rf_rate_override=args.rf_rate,
        dry_run=args.dry_run,
        export_csv=args.export,
        db_path=args.db,
        verbose=args.verbose,
    )

    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
