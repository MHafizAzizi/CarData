#!/bin/bash
# Self-healing carbase crawl. Restarts on crash, skips already-stored rows.
# Splits makes into small batches so a crash loses minimal progress.

MAKES=(
    perodua peugeot porsche proton renault
    subaru suzuki toyota volkswagen volvo
    # also retry nissan (died mid-crawl)
    nissan
)

for make in "${MAKES[@]}"; do
    echo "=== CRAWLING $make $(date) ==="
    timeout 600 python -u src/scrape_carbase_specs.py --makes "$make" 2>&1
    EXIT=$?
    if [ $EXIT -eq 124 ]; then
        echo "!!! $make TIMED OUT (>600s) — moving to next make $(date) !!!"
    elif [ $EXIT -ne 0 ]; then
        echo "!!! $make CRASHED (exit=$EXIT) — moving to next make $(date) !!!"
    else
        echo "--- $make done $(date) ---"
    fi
    sleep 2
done

echo "=== ALL MAKES DONE ==="
python -c "
import sqlite3
conn = sqlite3.connect('data/reference/carbase_specs.db')
c = conn.cursor()
c.execute('SELECT COUNT(*) FROM model_specs')
print(f'Final total: {c.fetchone()[0]} rows')
conn.close()
"
