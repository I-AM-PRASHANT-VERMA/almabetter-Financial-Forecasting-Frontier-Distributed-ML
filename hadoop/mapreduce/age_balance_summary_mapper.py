#!/usr/bin/env python
import csv
import sys


def to_age_band(age_value: int) -> str:
    # Fixed bands make the final summary easier to explain in the report than raw ages.
    if age_value < 30:
        return "18-29"
    if age_value < 40:
        return "30-39"
    if age_value < 50:
        return "40-49"
    if age_value < 60:
        return "50-59"
    return "60+"


reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    if len(row) != 17:
        continue

    age = row[0].strip()
    balance = row[5].strip()

    if not age or not balance:
        continue

    age_band = to_age_band(int(age))
    # The reducer receives balance plus count so it can print one average per age band.
    print("{}\t{}\t1".format(age_band, balance))
