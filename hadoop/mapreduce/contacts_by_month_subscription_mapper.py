#!/usr/bin/env python
import csv
import sys


reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    if len(row) != 17:
        continue

    month = row[10].strip()
    subscription = row[16].strip()

    if not month or not subscription:
        continue

    # Month and target are paired so the reducer can count each campaign outcome bucket.
    print("{}|{}\t1".format(month, subscription))
