#!/usr/bin/env python
"""Emit job, balance, and count values for the job-level average."""

import csv
import sys


reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    if len(row) != 17:
        continue

    job = row[1].strip()
    balance = row[5].strip()

    if not job or not balance:
        continue

    # The reducer needs the balance plus a row count so it can calculate the final average.
    print("{}\t{}\t1".format(job, balance))
