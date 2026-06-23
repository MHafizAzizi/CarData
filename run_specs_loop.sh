#!/bin/bash
# Self-healing specs-only loop. Kills stalled batches (>12min) and restarts.
TIMEOUT=720  # 12 minutes per batch (normal ~7min, generous margin)

while python src/recheck.py --method html --specs-only --dry-run --category cars 2>&1 | grep -q "Due for re-check: [1-9]"; do
    echo "=== BATCH START $(date) ==="
    timeout $TIMEOUT python src/recheck.py --method html --specs-only --limit 100 --category cars 2>&1 | tail -5
    EXIT=$?
    if [ $EXIT -eq 124 ]; then
        echo "!!! BATCH STALLED (>${TIMEOUT}s) — auto-restarting $(date) !!!"
        sleep 10
    else
        echo "--- batch done $(date) ---"
    fi
done
echo "=== ALL DONE ==="
