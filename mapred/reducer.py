#!/usr/bin/env python3
import sys

SUPPORT_THRESHOLD = 1000

current_item = None
current_count = 0

def print_current_item():
    if current_item is not None and current_count >= SUPPORT_THRESHOLD:
        print(f'{current_item}\t{current_count}')

for line in sys.stdin:
    item, count = line.strip().split('\t')
    count = int(count)

    if current_item == item:
        current_count += count
    else:
        print_current_item()
        current_item = item
        current_count = count

print_current_item()
