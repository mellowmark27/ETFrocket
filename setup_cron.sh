#!/usr/bin/env bash
# ============================================================
# MomentumLens — Local / VPS Cron Setup
# ============================================================
# Run this script once to install a crontab entry that triggers
# the daily pipeline automatically after LSE close.
#
# Usage:
#   chmod +x setup_cron.sh
#   ./setup_cron.sh
#
# To remove later:
#   crontab -e   (delete the momentum_lens line)
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$(which python3)"
RUNNER="$SCRIPT_DIR/run_daily.py"
LOG_DIR="$SCRIPT_DIR/logs"

mkdir -p "$LOG_DIR"

# ── Cron schedule ──────────────────────────────────────────
# Run Monday–Friday at 18:30 local time.
# Adjust the time to suit your timezone (LSE closes at 4:30pm UK).
# Format: minute hour day-of-month month day-of-week
CRON_SCHEDULE="30 18 * * 1-5"

# ── Full cron command ──────────────────────────────────────
CRON_CMD="$CRON_SCHEDULE cd $SCRIPT_DIR && $PYTHON $RUNNER --export >> $LOG_DIR/cron.log 2>&1"

# ── Install into crontab ───────────────────────────────────
# Preserve existing crontab entries, append ours if not already present
EXISTING=$(crontab -l 2>/dev/null || echo "")

if echo "$EXISTING" | grep -q "momentum_lens"; then
    echo "✓ Cron entry already exists. No changes made."
    echo ""
    echo "Current momentum_lens cron entries:"
    echo "$EXISTING" | grep "momentum_lens"
else
    (echo "$EXISTING"; echo "# MomentumLens nightly pipeline"; echo "$CRON_CMD") | crontab -
    echo "✓ Cron entry installed:"
    echo "  $CRON_CMD"
fi

echo ""
echo "To verify: crontab -l | grep momentum"
echo "To test now: cd $SCRIPT_DIR && $PYTHON $RUNNER --dry-run"
echo ""
echo "Log output will appear in: $LOG_DIR/cron.log"
