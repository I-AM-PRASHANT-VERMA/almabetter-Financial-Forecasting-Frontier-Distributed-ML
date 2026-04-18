#!/usr/bin/env python
import sys


current_age_band = None
balance_sum = 0.0
client_count = 0

for line in sys.stdin:
    age_band, balance, count = line.strip().split("\t")
    balance_value = float(balance)
    count_value = int(count)

    if current_age_band != age_band and current_age_band is not None:
        # Each band is closed out as soon as the reducer sees the next sorted key.
        average_balance = balance_sum / client_count if client_count else 0.0
        print("{0}\t{1:.2f}\t{2}".format(current_age_band, average_balance, client_count))
        balance_sum = 0.0
        client_count = 0

    current_age_band = age_band
    balance_sum += balance_value
    client_count += count_value

if current_age_band is not None:
    average_balance = balance_sum / client_count if client_count else 0.0
    print("{0}\t{1:.2f}\t{2}".format(current_age_band, average_balance, client_count))
