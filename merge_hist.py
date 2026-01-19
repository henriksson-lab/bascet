#!/usr/bin/env python3

import sys
import os
from pathlib import Path
from collections import defaultdict

def merge_hist_files(data_dir, threshold):
    counts = defaultdict(int)

    hist_files = list(Path(data_dir).glob("*.hist"))

    if not hist_files:
        print(f"No .hist files found in {data_dir}", file=sys.stderr)
        return

    print(f"Found {len(hist_files)} hist file(s)")

    for hist_file in hist_files:
        print(f"Reading {hist_file}")
        with open(hist_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) == 2:
                    key, count = parts[0], int(parts[1])
                    counts[key] += count

    print(f"Total unique keys: {len(counts)}")

    filtered_keys = [key for key, count in counts.items() if count >= threshold]

    print(f"Keys with count >= {threshold}: {len(filtered_keys)}")

    output_file = Path(data_dir) / "filter.txt"
    with open(output_file, 'w') as f:
        for key in sorted(filtered_keys):
            f.write(f"{key}\n")

    print(f"Wrote {len(filtered_keys)} keys to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: merge_hist.py [data_dir] [threshold]")
        print("  data_dir:  directory containing .hist files (default: ./data)")
        print("  threshold: minimum sum of counts to include key (default: 100)")
        sys.exit(1)

    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    threshold = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    merge_hist_files(data_dir, threshold)
