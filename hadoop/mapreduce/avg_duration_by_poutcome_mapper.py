#!/usr/bin/env python
import csv
import sys


reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    if len(row) != 17:
        continue

    poutcome = row[15].strip()
    duration = row[11].strip()

    if not poutcome or not duration:
        continue

    # Duration is passed with a counter so the reducer can calculate a proper mean.
    print("{}\t{}\t1".format(poutcome, duration))
