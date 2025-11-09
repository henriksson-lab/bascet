import csv
import re
import sys
from pathlib import Path

def normalize_well(well):
    match = re.match(r'^([A-Za-z]+)(\d+)$', well)
    if match:
        letter, number = match.groups()
        return f"{letter}{int(number):02d}"
    return well

filepath = Path(sys.argv[1])
output_filepath = filepath.with_suffix(filepath.suffix + '.sorted')

with open(filepath, 'r') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

for row in rows:
    row['well'] = normalize_well(row['well'])

rows.sort(key=lambda row: (row['well'], int(row['bci'])))

for i, row in enumerate(rows, start=1):
    row['bci'] = str(i)

fieldnames = ['bci', 'sequence', 'uid', 'well', 'stype']
with open(output_filepath, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)