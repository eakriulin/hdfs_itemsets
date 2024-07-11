#!/usr/bin/env python3
import sys
from itertools import combinations

for line in sys.stdin:
    items = line.strip().split(',')
    for triplet in combinations(items, 3):
        key = ','.join(triplet)
        print(f'{key}\t1')