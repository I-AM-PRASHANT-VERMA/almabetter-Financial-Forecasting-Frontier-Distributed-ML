#!/usr/bin/env python
import sys


current_poutcome = None
duration_sum = 0.0
client_count = 0

for line in sys.stdin:
    poutcome, duration, count = line.strip().split("\t")
    duration_value = float(duration)
    count_value = int(count)

    if current_poutcome != poutcome and current_poutcome is not None:
        # The sorted reducer stream lets each previous-campaign outcome be finished one by one.
        average_duration = duration_sum / client_count if client_count else 0.0
        print("{}\t{:.2f}\t{}".format(current_poutcome, average_duration, client_count))
        duration_sum = 0.0
        client_count = 0

    current_poutcome = poutcome
    duration_sum += duration_value
    client_count += count_value

if current_poutcome is not None:
    average_duration = duration_sum / client_count if client_count else 0.0
    print("{}\t{:.2f}\t{}".format(current_poutcome, average_duration, client_count))
