#!/usr/bin/env python
import sys


current_key = None
current_count = 0

for line in sys.stdin:
    key, count = line.strip().split("\t")
    count_value = int(count)

    if current_key != key and current_key is not None:
        # Once the key changes, the grouped month and subscription total is ready to write out.
        month, subscription = current_key.split("|")
        print("{}\t{}\t{}".format(month, subscription, current_count))
        current_count = 0

    current_key = key
    current_count += count_value

if current_key is not None:
    month, subscription = current_key.split("|")
    print("{}\t{}\t{}".format(month, subscription, current_count))
