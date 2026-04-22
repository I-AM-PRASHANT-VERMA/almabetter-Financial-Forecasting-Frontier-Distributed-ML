#!/usr/bin/env python
import sys


current_key = None
current_count = 0

for line in sys.stdin:
    key, count = line.strip().split("\t")
    count_value = int(count)

    if current_key != key and current_key is not None:
        # The combined key is split back out only when the grouped total is ready to print.
        education, housing = current_key.split("|")
        print("{}\t{}\t{}".format(education, housing, current_count))
        current_count = 0

    current_key = key
    current_count += count_value

if current_key is not None:
    education, housing = current_key.split("|")
    print("{}\t{}\t{}".format(education, housing, current_count))
