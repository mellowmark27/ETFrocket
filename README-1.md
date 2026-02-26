# MomentumLens — Phase 1: LSE ETF Dual Momentum Scanner

A Python script that fetches ETF performance data from **JustETF**, applies
**Gary Antonacci's dual momentum** framework, and outputs a ranked CSV of
LSE-listed ETF candidates.

---

## What it does

1. **Fetches** all long-only ETFs listed on the London Stock Exchange from
   JustETF's screener API (typically 500–900 ETFs depending on filters).
2. **Fetches** the current 3-month UK Gilt yield from FRED as the risk-free
   rate benchmark.
3. **Scores** every ETF using dual momentum:
   - **Relative momentum** — percentile rank within asset-class peer group
   - **Absolute momentum** — is the ETF outperforming cash (the risk-free rate)?
   - **Dual score** — combined 0–100 score; zero if absolute momentum is OFF
4. **Outputs** a ranked CSV and prints a summary table to the console.

---

## Quick start

### 1. Install dependencies

```bash
pip install requests pandas beautifulsoup4 lxml
```

### 2. Run with defaults (12-1M lookback, no AUM filter)

```bash
python momentum_scanner.py
```

### 3. Common options

```bash
# Use 6-month lookback (more responsive, noisier)
python momentum_scanner.py --lookback 6

# Only include ETFs with AUM ≥ £500M (more liquid)
python momentum_scanner.py --min-aum 500

# Override the risk-free rate manually (e.g. if FRED is unreachable)
python momentum_scanner.py --rf-rate 4.75

# Show top 30 candidates in console output
python momentum_scanner.py --top 30

# Custom output filename
python momentum_scanner.py --output results_jan2025.csv

# Combine options
python momentum_scanner.py --lookback 12 --min-aum 200 --top 25
```

---

## Output CSV columns

| Column            | Description                                              |
|-------------------|----------------------------------------------------------|
| `rank`            | Overall rank by dual score (1 = highest)                 |
| `isin`            | ISIN code                                                |
| `ticker`          | LSE ticker (e.g. SWDA, VWRL)                             |
| `name`            | Full ETF name                                            |
| `asset_class`     | e.g. Equity, Bonds, Commodities                          |
| `sub_asset`       | e.g. World, US, EM, Government Bonds                     |
| `region`          | Geographic focus                                         |
| `peer_group`      | Peer group used for relative ranking                     |
| `aum_gbp_m`       | Assets under management in £M                            |
| `ter_pct`         | Total expense ratio %                                    |
| `distribution`    | Accumulating or Distributing                             |
| `ret_1m` … `ret_12m` | Period returns in %                                   |
| `momentum_return` | Lookback return minus 1M (skip-month adjusted)           |
| `rel_score`       | Relative rank within peer group (0–100)                  |
| `abs_label`       | Absolute momentum: ON or OFF                             |
| `dual_score`      | Combined dual momentum score (0–100)                     |
| `signal_class`    | Strong Candidate / Moderate / Weak / No Signal           |

---

## Dual momentum logic

```
momentum_return = R(lookback) − R(1M)       # skip-month adjusted

rel_score       = percentile_rank(momentum_return, within peer_group) × 100

abs_signal      = 1 if R(lookback) > rf_rate else 0

dual_score      = rel_score × abs_signal    # 0 if abs momentum is OFF

Candidates:     dual_score ≥ 70
Strong:         dual_score ≥ 80
```

**Why 12-1M?**  
The classic Jegadeesh-Titman skip-month adjustment (using 12-month return
minus the most recent 1-month) reduces short-term mean-reversion contaminating
the signal. It's the standard formulation in momentum literature.

**Why dual?**  
Relative momentum alone tells you *which* asset to be in — but not *whether*
to be in the market at all. Absolute momentum acts as a bear market filter:
if an ETF's own return is below cash, you hold cash instead, avoiding large
drawdowns.

---

## JustETF API notes

The script calls JustETF's internal screener JSON API (`/api/etfs`).
This is the same endpoint used by their website's screener.

**Polite usage:**
- Requests are rate-limited to one per 1.5 seconds.
- The full LSE universe typically requires 6–12 requests (pagination).
- Run at most once per day; JustETF data updates daily.

**If you get HTTP 403 or 429:**
- You may have been rate-limited. Wait 10–15 minutes and try again.
- Add `--min-aum 100` to reduce the dataset size.
- Consider adding a longer delay by editing `REQUEST_DELAY` in the script.

**API endpoint may change:**  
JustETF is a commercial product and their internal API is undocumented.
If the script breaks, check the GitHub issues on `druzsan/justetf-scraping`
for community-discovered endpoint updates.

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `HTTP 403` | JustETF blocked the request. Wait and retry, or adjust User-Agent. |
| `No ETFs returned` | Check internet connection. Try the URL in a browser. |
| `ret_12m all NaN` | Performance data missing — try `--min-aum 50` to filter newer ETFs. |
| FRED risk-free fetch fails | Use `--rf-rate 4.75` to supply manually. |
| `JSONDecodeError` | JustETF API endpoint changed. Check `/api/etfs` URL. |

---

## Disclaimer

This tool is for **research and educational purposes only**.  
Nothing here constitutes financial advice.  
Past momentum signals do not guarantee future returns.  
Dual momentum strategies can experience significant drawdowns.  
Always do your own research before making any investment decisions.

---

## What's next (Phase 2)

- Persist daily snapshots to SQLite database
- Schedule nightly runs via cron / GitHub Actions  
- Build 3-month historical score table
- Visualise score trends per ETF

