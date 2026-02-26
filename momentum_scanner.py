"""
MomentumLens — Phase 1: LSE ETF Dual Momentum Scanner
======================================================
Fetches ETF performance data from JustETF, applies Gary Antonacci's
dual momentum framework, and outputs a ranked CSV of candidates.

Usage:
    python momentum_scanner.py                  # run with defaults
    python momentum_scanner.py --lookback 6     # use 6-1M lookback
    python momentum_scanner.py --min-aum 500    # filter ETFs < £500M AUM
    python momentum_scanner.py --output my_results.csv

Requirements:
    pip install requests pandas beautifulsoup4 lxml

Author: MomentumLens
"""

import argparse
import time
import sys
import logging
from datetime import date, datetime
from typing import Optional

import requests
import pandas as pd

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("momentum_lens")


# ─────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────

JUSTETF_SCREENER_URL = "https://www.justetf.com/uk/find-etf.html"

# JustETF internal API — returns JSON for screener results
JUSTETF_API_URL = "https://www.justetf.com/api/etfs"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.justetf.com/uk/find-etf.html",
    "X-Requested-With": "XMLHttpRequest",
}

# Polite crawl delay between requests (seconds)
REQUEST_DELAY = 1.5

# Asset class groupings used to form peer groups for relative momentum
# Maps JustETF assetClass values → peer group label
PEER_GROUP_MAP = {
    "Equity":            "Equity",
    "Bonds":             "Bonds",
    "Real Estate":       "Real Estate",
    "Commodities":       "Commodities",
    "Money Market":      "Money Market",
    "Multi Asset":       "Multi Asset",
    "Alternatives":      "Alternatives",
    "Cryptocurrencies":  "Cryptocurrencies",
}

# Fallback peer group if asset class not in map
DEFAULT_PEER_GROUP = "Other"

# Dual momentum candidate threshold (0–100)
CANDIDATE_THRESHOLD = 70


# ─────────────────────────────────────────────────────────
# STEP 1: FETCH ETF LIST FROM JUSTETF (LSE / XLON only)
# ─────────────────────────────────────────────────────────

def fetch_lse_etfs(min_aum_gbp: float = 0) -> pd.DataFrame:
    """
    Fetch all long-only ETFs listed on LSE (exchange=XLON) from JustETF.

    JustETF exposes a paginated JSON API used by their screener.
    We paginate through all results collecting ISIN, name, ticker,
    asset class, AUM, TER, and performance figures.

    Parameters
    ----------
    min_aum_gbp : float
        Minimum AUM in GBP millions. ETFs below this are excluded.

    Returns
    -------
    pd.DataFrame with one row per ETF.
    """
    log.info("Fetching LSE ETF list from JustETF...")

    params = {
        "exchange":    "XLON",       # London Stock Exchange only
        "groupField":  "none",
        "sortField":   "fundSize",
        "sortOrder":   "desc",
        "offset":      0,
        "limit":       100,          # max rows per page
        "lang":        "en",
        "country":     "GB",
        # Long-only, plain ETFs (exclude leveraged / inverse)
        "etfType":     "ETF",
    }

    all_etfs = []
    page = 0

    while True:
        params["offset"] = page * params["limit"]
        log.info(f"  Page {page + 1}: offset={params['offset']}")

        try:
            resp = requests.get(
                JUSTETF_API_URL,
                params=params,
                headers=HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.HTTPError as e:
            log.error(f"HTTP error fetching ETF list: {e}")
            log.error("JustETF may have changed their API. See README for troubleshooting.")
            raise
        except requests.exceptions.RequestException as e:
            log.error(f"Network error: {e}")
            raise
        except ValueError:
            log.error("JustETF did not return valid JSON. The API endpoint may have changed.")
            raise

        etfs = data.get("etfs", [])
        total = data.get("total", 0)

        if not etfs:
            break

        all_etfs.extend(etfs)
        log.info(f"    Collected {len(all_etfs)} / {total} ETFs so far")

        if len(all_etfs) >= total:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    log.info(f"Total ETFs fetched: {len(all_etfs)}")

    if not all_etfs:
        raise RuntimeError("No ETFs returned. Check network connection and JustETF API status.")

    df = _parse_etf_list(all_etfs)

    # Apply AUM filter
    if min_aum_gbp > 0:
        before = len(df)
        df = df[df["aum_gbp_m"] >= min_aum_gbp].copy()
        log.info(f"AUM filter (≥ £{min_aum_gbp}M): {before} → {len(df)} ETFs")

    return df


def _parse_etf_list(raw: list) -> pd.DataFrame:
    """Parse the raw JSON list of ETFs into a clean DataFrame."""
    rows = []
    for etf in raw:
        # Performance figures — JustETF returns as percentages (e.g. 12.4 means 12.4%)
        perf = etf.get("performance", {})

        row = {
            "isin":           etf.get("isin", ""),
            "name":           etf.get("name", ""),
            "ticker":         _extract_lse_ticker(etf),
            "asset_class":    etf.get("assetClass", ""),
            "sub_asset":      etf.get("subAssetClass", ""),
            "region":         etf.get("region", ""),
            "index_name":     etf.get("replicationIndex", ""),
            "distribution":   etf.get("distributionPolicy", ""),   # Accumulating / Distributing
            "replication":    etf.get("replication", ""),           # Physical / Synthetic
            "ter_pct":        _safe_float(etf.get("ter")),          # Total Expense Ratio %
            "aum_gbp_m":      _safe_float(etf.get("fundSize")),     # AUM in £M (JustETF uses local currency for UK)
            "inception_date": etf.get("launchDate", ""),
            # Performance columns (percentage returns)
            "ret_1m":         _safe_float(perf.get("m1")),
            "ret_3m":         _safe_float(perf.get("m3")),
            "ret_6m":         _safe_float(perf.get("m6")),
            "ret_12m":        _safe_float(perf.get("m12")),
            "ret_ytd":        _safe_float(perf.get("ytd")),
            "ret_3y":         _safe_float(perf.get("m36")),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    log.info(f"Parsed {len(df)} ETFs. Columns: {list(df.columns)}")
    return df


def _extract_lse_ticker(etf: dict) -> str:
    """Extract the LSE ticker from the exchanges list."""
    for exc in etf.get("exchanges", []):
        if exc.get("exchange") == "XLON":
            return exc.get("symbol", "")
    return etf.get("symbol", "")  # fallback


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None if not possible."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────
# STEP 2: FETCH RISK-FREE RATE (3M UK GILT PROXY)
# ─────────────────────────────────────────────────────────

def fetch_risk_free_rate() -> float:
    """
    Fetch the current annualised 3-month UK Gilt yield from FRED (St. Louis Fed).
    This serves as the absolute momentum benchmark (risk-free rate).

    Uses FRED series TB3MS (3-Month Treasury Bill) as a fallback.
    For UK-specific data, series GB3MTN=X or BoE publication would be preferred
    but FRED is the easiest free public API.

    Falls back to a hardcoded estimate if the API is unreachable.

    Returns
    -------
    float : annualised yield in % (e.g. 4.5 means 4.5%)
    """
    FRED_SERIES = "GBATB3M"   # UK 3-Month Treasury Bill from FRED
    FRED_FALLBACK = "TB3MS"   # US 3M T-Bill as secondary fallback
    HARDCODED_FALLBACK = 4.5  # Approximate UK rate as of early 2025

    for series in [FRED_SERIES, FRED_FALLBACK]:
        url = (
            f"https://fred.stlouisfed.org/graph/fredgraph.json"
            f"?id={series}&vintage_date={date.today().isoformat()}"
        )
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            observations = data.get("observations", [])
            if observations:
                latest = observations[-1].get("value")
                rf = float(latest)
                log.info(f"Risk-free rate ({series}): {rf:.2f}%")
                return rf
        except Exception as e:
            log.warning(f"Could not fetch {series} from FRED: {e}")

    log.warning(f"Using hardcoded risk-free rate fallback: {HARDCODED_FALLBACK}%")
    return HARDCODED_FALLBACK


# ─────────────────────────────────────────────────────────
# STEP 3: DUAL MOMENTUM SCORING ENGINE
# ─────────────────────────────────────────────────────────

def compute_dual_momentum(
    df: pd.DataFrame,
    lookback_months: int = 12,
    risk_free_rate: float = 4.5,
) -> pd.DataFrame:
    """
    Apply Antonacci's dual momentum framework to the ETF universe.

    Two signals are computed:

    1. RELATIVE MOMENTUM
       Rank each ETF within its asset-class peer group by lookback return.
       Classic lookback = 12-month return minus 1-month return (12-1M).
       Shorter lookbacks (6-1M, 3-1M) are supported via --lookback flag.
       Result: `rel_score` = 0–100 (percentile within peer group)

    2. ABSOLUTE MOMENTUM
       Compare each ETF's lookback return against the risk-free rate.
       If return > rf_rate → abs_signal = 1 (positive momentum above cash)
       If return ≤ rf_rate → abs_signal = 0 (negative / sub-cash momentum)

    3. COMBINED DUAL SCORE
       dual_score = rel_score × abs_signal
       Range: 0–100. Zero if absolute momentum is off.
       Candidates: dual_score ≥ 70

    Parameters
    ----------
    df : pd.DataFrame
        Output from fetch_lse_etfs()
    lookback_months : int
        Primary lookback period (12, 6, or 3). Default 12.
    risk_free_rate : float
        Annual risk-free rate in % (e.g. 4.5)

    Returns
    -------
    pd.DataFrame with momentum scores added, sorted by dual_score descending.
    """
    log.info(f"Computing dual momentum (lookback={lookback_months}M, rf={risk_free_rate:.2f}%)")

    df = df.copy()

    # ── Select primary return column based on lookback ──────────────────
    col_map = {12: "ret_12m", 6: "ret_6m", 3: "ret_3m"}
    if lookback_months not in col_map:
        raise ValueError(f"lookback_months must be 12, 6, or 3. Got: {lookback_months}")

    primary_col = col_map[lookback_months]

    # ── Compute momentum return: primary - 1M (skip-month adjustment) ────
    # The 12-1M formula avoids short-term reversal contaminating signal
    df["momentum_return"] = df[primary_col] - df["ret_1m"].fillna(0)

    # ── Drop ETFs with no primary return data ────────────────────────────
    n_before = len(df)
    df = df.dropna(subset=["momentum_return"]).copy()
    n_dropped = n_before - len(df)
    if n_dropped > 0:
        log.info(f"Dropped {n_dropped} ETFs with missing {primary_col} data")

    # ── Assign peer groups ────────────────────────────────────────────────
    df["peer_group"] = df["asset_class"].map(PEER_GROUP_MAP).fillna(DEFAULT_PEER_GROUP)

    # ── SIGNAL 1: Relative momentum (percentile within peer group) ───────
    df["rel_score"] = (
        df.groupby("peer_group")["momentum_return"]
        .rank(pct=True) * 100
    ).round(1)

    # Log peer group sizes
    peer_sizes = df.groupby("peer_group").size()
    log.info("Peer group sizes:\n" + peer_sizes.to_string())

    # ── SIGNAL 2: Absolute momentum ───────────────────────────────────────
    # Compare full lookback return (not skip-month adjusted) vs risk-free
    df["abs_signal"] = (df[primary_col] > risk_free_rate).astype(int)
    df["abs_label"]  = df["abs_signal"].map({1: "ON", 0: "OFF"})
    log.info(
        f"Absolute momentum: {df['abs_signal'].sum()} ON / "
        f"{(df['abs_signal'] == 0).sum()} OFF"
    )

    # ── SIGNAL 3: Combined dual momentum score ────────────────────────────
    df["dual_score"] = (df["rel_score"] * df["abs_signal"]).round(1)

    # ── Classification ────────────────────────────────────────────────────
    def classify(row):
        if row["dual_score"] >= 80:
            return "Strong Candidate"
        elif row["dual_score"] >= CANDIDATE_THRESHOLD:
            return "Moderate Candidate"
        elif row["abs_signal"] == 1:
            return "Weak Signal"
        else:
            return "No Signal (abs OFF)"

    df["signal_class"] = df.apply(classify, axis=1)

    # ── Sort by dual score descending ─────────────────────────────────────
    df = df.sort_values("dual_score", ascending=False).reset_index(drop=True)
    df.index += 1  # 1-based rank

    n_strong   = (df["signal_class"] == "Strong Candidate").sum()
    n_moderate = (df["signal_class"] == "Moderate Candidate").sum()
    log.info(
        f"Results: {n_strong} Strong + {n_moderate} Moderate candidates "
        f"(threshold ≥ {CANDIDATE_THRESHOLD})"
    )

    return df


# ─────────────────────────────────────────────────────────
# STEP 4: OUTPUT
# ─────────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "isin", "ticker", "name",
    "asset_class", "sub_asset", "region",
    "peer_group",
    "aum_gbp_m", "ter_pct",
    "distribution", "replication",
    "ret_1m", "ret_3m", "ret_6m", "ret_12m", "ret_ytd",
    "momentum_return",
    "rel_score", "abs_label", "dual_score", "signal_class",
]


def save_results(df: pd.DataFrame, output_path: str, risk_free_rate: float, lookback: int):
    """Save scored ETF DataFrame to CSV with a metadata header."""

    # Only keep columns that actually exist in the DataFrame
    cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    out = df.reset_index().rename(columns={"index": "rank"})[["rank"] + cols]

    # Write metadata as commented lines at top
    meta_lines = [
        f"# MomentumLens — LSE ETF Dual Momentum Scanner",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"# Lookback window: {lookback}-1M",
        f"# Risk-free rate (abs. momentum benchmark): {risk_free_rate:.2f}%",
        f"# Dual score threshold for candidates: ≥ {CANDIDATE_THRESHOLD}",
        f"# Total ETFs scored: {len(df)}",
        f"# Candidates (dual_score ≥ {CANDIDATE_THRESHOLD}): "
        f"{(df['dual_score'] >= CANDIDATE_THRESHOLD).sum()}",
        f"#",
        f"# DISCLAIMER: For research purposes only. Not financial advice.",
        f"# Data source: JustETF (justetf.com). Accuracy not guaranteed.",
        f"#",
    ]

    with open(output_path, "w", encoding="utf-8") as f:
        for line in meta_lines:
            f.write(line + "\n")
        out.to_csv(f, index=False)

    log.info(f"Results saved to: {output_path}")


def print_summary(df: pd.DataFrame, top_n: int = 20):
    """Print a formatted summary table to the console."""

    candidates = df[df["dual_score"] >= CANDIDATE_THRESHOLD].head(top_n)

    print("\n" + "═" * 100)
    print("  MomentumLens — Top Dual Momentum Candidates (LSE ETFs)")
    print("═" * 100)

    if candidates.empty:
        print("  No candidates found above threshold.")
        print("═" * 100)
        return

    fmt = "{:<4} {:<12} {:<45} {:<14} {:>7} {:>7} {:>7} {:>7} {:>5} {:>4} {:>8} {:<20}"
    header = fmt.format(
        "Rank", "Ticker", "Name", "Asset Class",
        "12M Ret", "6M Ret", "3M Ret", "Mom Ret",
        "Rel", "Abs", "DualScore", "Signal"
    )
    print(header)
    print("-" * 100)

    for rank, row in candidates.iterrows():
        print(fmt.format(
            rank,
            str(row.get("ticker", ""))[:12],
            str(row.get("name", ""))[:44],
            str(row.get("asset_class", ""))[:14],
            _fmt_pct(row.get("ret_12m")),
            _fmt_pct(row.get("ret_6m")),
            _fmt_pct(row.get("ret_3m")),
            _fmt_pct(row.get("momentum_return")),
            f"{row['rel_score']:.0f}",
            row["abs_label"],
            f"{row['dual_score']:.1f}",
            str(row["signal_class"])[:20],
        ))

    print("═" * 100)
    print(f"  Showing top {min(top_n, len(candidates))} of "
          f"{len(df[df['dual_score'] >= CANDIDATE_THRESHOLD])} candidates.\n")


def _fmt_pct(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "  N/A "
    return f"{val:+.1f}%"


# ─────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="MomentumLens — LSE ETF Dual Momentum Scanner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python momentum_scanner.py
  python momentum_scanner.py --lookback 6 --min-aum 500
  python momentum_scanner.py --output results_2025.csv --top 30
  python momentum_scanner.py --rf-rate 4.75
        """
    )
    parser.add_argument(
        "--lookback", type=int, choices=[3, 6, 12], default=12,
        help="Momentum lookback in months (default: 12, classic dual momentum)"
    )
    parser.add_argument(
        "--min-aum", type=float, default=0,
        metavar="GBP_MILLIONS",
        help="Minimum AUM in GBP millions (default: 0, no filter)"
    )
    parser.add_argument(
        "--rf-rate", type=float, default=None,
        metavar="PERCENT",
        help=(
            "Override risk-free rate in %% (e.g. 4.5). "
            "If not set, fetched live from FRED."
        )
    )
    parser.add_argument(
        "--output", type=str,
        default=f"momentum_scores_{date.today().isoformat()}.csv",
        help="Output CSV file path"
    )
    parser.add_argument(
        "--top", type=int, default=20,
        help="Number of top candidates to print to console (default: 20)"
    )
    parser.add_argument(
        "--no-fetch-rf", action="store_true",
        help="Skip live risk-free rate fetch, use hardcoded fallback (4.5%%)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("\n  MomentumLens — LSE ETF Dual Momentum Scanner")
    print(f"  {date.today().isoformat()}  |  Lookback: {args.lookback}-1M  |  Min AUM: £{args.min_aum}M\n")

    # 1. Fetch ETFs
    df_etfs = fetch_lse_etfs(min_aum_gbp=args.min_aum)

    # 2. Get risk-free rate
    if args.rf_rate is not None:
        rf = args.rf_rate
        log.info(f"Using user-supplied risk-free rate: {rf:.2f}%")
    elif args.no_fetch_rf:
        rf = 4.5
        log.info(f"Using hardcoded risk-free rate: {rf:.2f}%")
    else:
        rf = fetch_risk_free_rate()

    # 3. Score
    df_scored = compute_dual_momentum(
        df_etfs,
        lookback_months=args.lookback,
        risk_free_rate=rf,
    )

    # 4. Print summary
    print_summary(df_scored, top_n=args.top)

    # 5. Save CSV
    save_results(df_scored, args.output, rf, args.lookback)

    print(f"  Full results: {args.output}\n")


if __name__ == "__main__":
    main()
