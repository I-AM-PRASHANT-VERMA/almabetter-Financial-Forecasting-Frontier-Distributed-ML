#!/usr/bin/env python
import csv
import sys


reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    if len(row) != 17:
        continue

    education = row[3].strip()
    housing = row[6].strip()

    if not education or not housing:
        continue

    # Joining the two fields into one key keeps the reducer logic simple.
    print("{}|{}\t1".format(education, housing))
