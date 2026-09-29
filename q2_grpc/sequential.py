"""
The Q8 analytics without gRPC: reads a dataset file and prints the result in the HW2 format.
Used to check that weather_stats.py (our Python port of HW2's analytics) gives the same output as
HW2's C++ sequential program.

usage: python sequential.py <input_file>
"""

import sys
from weather_stats import Stats, read_dataset


def main():
    if len(sys.argv) < 2:
        print("usage: python sequential.py <input_file>", file=sys.stderr)
        sys.exit(1)
    has_header, N, K, S, records = read_dataset(sys.argv[1])
    if not has_header:
        return   # like HW2: an empty file prints nothing
    stats = Stats(S)
    for m in records:
        stats.update(m)
    sys.stdout.write(stats.format_results(K))


if __name__ == "__main__":
    main()
