#!/usr/bin/env python3
"""
Q8 dataset generator - Large-Scale Weather and Environmental Data Analytics

Generates a reproducible input file in the format:

    N K S
    timestamp station_id temperature humidity pressure rainfall wind_speed
    ... (N lines)

Usage:
    python3 generate_dataset.py --n 1000 --k 5 --s 20 --seed 42 --out data.txt

Value ranges (the assignment does not specify these - chosen here and
documented so every generated dataset stays consistent):

    station_id  : integer, uniform in [0, S-1]
    timestamp   : integer, uniform in [0, N * TIMESTAMP_SPREAD_FACTOR]
                  (TIMESTAMP_SPREAD_FACTOR default 10, so records land with
                  a handful per 60-second interval on average - enough
                  density to exercise BUSIEST_INTERVAL without every record
                  landing in the same interval)
    temperature : float, uniform in [-20.0, 50.0] deg C
                  (comfortably straddles the >=40.0 / <=0.0 "extreme
                  temperature event" boundaries so both fire in practice)
    humidity    : float, uniform in [0.0, 100.0] percent
    pressure    : float, uniform in [950.0, 1050.0] hPa
    rainfall    : float, uniform in [0.0, 50.0] mm, non-negative
    wind_speed  : float, uniform in [0.0, 40.0] km/h, non-negative

Floating-point fields are written with 2 decimal digits - this is just the
*generator's* input precision, unrelated to the assignment's required
6-decimal *output* formatting (that's printResults() in q8_common.cpp).

Reproducibility: uses Python's random.Random(seed), so the same
--n/--k/--s/--seed combination always produces byte-identical output.

Records are written to the output file one at a time as they're generated
(not accumulated in memory first), so this scales to large N without
holding the whole dataset in RAM.
"""

import argparse
import random


TIMESTAMP_SPREAD_FACTOR = 10

TEMPERATURE_RANGE = (-20.0, 50.0)
HUMIDITY_RANGE = (0.0, 100.0)
PRESSURE_RANGE = (950.0, 1050.0)
RAINFALL_RANGE = (0.0, 50.0)
WIND_SPEED_RANGE = (0.0, 40.0)


def generate(n, k, s, seed, f):
    rng = random.Random(seed)

    f.write(f"{n} {k} {s}\n")
    max_timestamp = max(n * TIMESTAMP_SPREAD_FACTOR, 1)

    for _ in range(n):
        timestamp = rng.randint(0, max_timestamp)
        station_id = rng.randint(0, s - 1)
        temperature = rng.uniform(*TEMPERATURE_RANGE)
        humidity = rng.uniform(*HUMIDITY_RANGE)
        pressure = rng.uniform(*PRESSURE_RANGE)
        rainfall = rng.uniform(*RAINFALL_RANGE)
        wind_speed = rng.uniform(*WIND_SPEED_RANGE)

        f.write(
            f"{timestamp} {station_id} "
            f"{temperature:.2f} {humidity:.2f} {pressure:.2f} "
            f"{rainfall:.2f} {wind_speed:.2f}\n"
        )


def main():
    parser = argparse.ArgumentParser(description="Generate a reproducible Q8 weather dataset.")
    parser.add_argument("--n", type=int, required=True, help="number of measurement records")
    parser.add_argument("--k", type=int, required=True, help="top-K stations to report")
    parser.add_argument("--s", type=int, required=True, help="number of distinct stations")
    parser.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    parser.add_argument("--out", type=str, required=True, help="output file path")
    args = parser.parse_args()

    if args.n < 0 or args.k < 0 or args.s <= 0:
        parser.error("n and k must be >= 0, s must be >= 1")

    with open(args.out, "w") as f:
        generate(args.n, args.k, args.s, args.seed, f)

    print(f"wrote {args.n} records (K={args.k}, S={args.s}, seed={args.seed}) to {args.out}")


if __name__ == "__main__":
    main()
