#!/usr/bin/env python
import sys


current_job = None
balance_sum = 0.0
client_count = 0

for line in sys.stdin:
    job, balance, count = line.strip().split("\t")
    balance_value = float(balance)
    count_value = int(count)

    if current_job != job and current_job is not None:
        # Hadoop streams sorted keys to the reducer, so each job can be finalized as soon as the key changes.
        average_balance = balance_sum / client_count if client_count else 0.0
        print("{}\t{:.2f}\t{}".format(current_job, average_balance, client_count))
        balance_sum = 0.0
        client_count = 0

    current_job = job
    balance_sum += balance_value
    client_count += count_value

if current_job is not None:
    average_balance = balance_sum / client_count if client_count else 0.0
    print("{}\t{:.2f}\t{}".format(current_job, average_balance, client_count))
