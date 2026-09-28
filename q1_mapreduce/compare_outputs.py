#!/usr/bin/env python3
"""
Compares two q8 output files (for example sequential vs mapreduce).

Why not just use diff?
Adding millions of floating point numbers in a different order gives slightly different
last digits. Example for 10M records (the true value is exactly ...538.54, because every
rainfall value has 2 decimals, so BOTH programs have a small rounding error):

    sequential:  TOTAL_RAINFALL 249966638.539998
    mapreduce:   TOTAL_RAINFALL 249966638.540026

A double has only ~15-16 significant digits, and this number needs 15 digits at 6 decimals,
so the last digits are rounding noise that depends on the order of the additions.
(hw2's own mpi program has the same difference with sequential when it uses a different
number of workers, so this is not something mapreduce specific.)

Rules:
    - both files must have the same number of lines and the same words on every line
    - labels and whole numbers (counts, station ids, timestamps, interval ids) must match exactly
    - decimal numbers must match with a relative tolerance of 1e-10

Prints one word and exits with 0 if the files match, or 1 if they don't:
    EXACT     the files are identical
    FP_CLOSE  only differences are floating point rounding within the tolerance
    FAIL      a real difference (the differing lines are printed to stderr)

Usage: python3 compare_outputs.py <expected_file> <actual_file>
"""

import sys

REL_TOL = 1e-10


def numbers_close(x, y):
    # whole numbers (no decimal point) must be exactly equal
    if "." not in x or "." not in y:
        return False
    try:
        fx = float(x)
        fy = float(y)
    except ValueError:
        return False
    return abs(fx - fy) <= REL_TOL * max(abs(fx), abs(fy), 1.0)


def main():
    if len(sys.argv) != 3:
        print("usage: python3 compare_outputs.py <expected_file> <actual_file>")
        sys.exit(2)

    expected = open(sys.argv[1]).read().splitlines()
    actual = open(sys.argv[2]).read().splitlines()

    if expected == actual:
        print("EXACT")
        sys.exit(0)

    if len(expected) != len(actual):
        print("FAIL")
        print(f"different number of lines: {len(expected)} vs {len(actual)}", file=sys.stderr)
        sys.exit(1)

    ok = True
    for line_a, line_b in zip(expected, actual):
        if line_a == line_b:
            continue

        words_a = line_a.split()
        words_b = line_b.split()
        same = len(words_a) == len(words_b)
        if same:
            for x, y in zip(words_a, words_b):
                if x != y and not numbers_close(x, y):
                    same = False
                    break

        if same:
            print(f"  rounding only: '{line_a}' vs '{line_b}'", file=sys.stderr)
        else:
            print(f"  DIFFERENT:     '{line_a}' vs '{line_b}'", file=sys.stderr)
            ok = False

    if ok:
        print("FP_CLOSE")
        sys.exit(0)

    print("FAIL")
    sys.exit(1)


if __name__ == "__main__":
    main()
