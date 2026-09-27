# Q8 — Large-Scale Weather and Environmental Data Analytics

Sequential and MPI implementations for HW2 Q8 (Real-World Applications
section). Computes total/average/min/max weather statistics, Top-K
stations, hottest/coldest measurements, the busiest 60-second interval,
and extreme-temperature event counts over a stream of weather station
measurements.

## Files

| File | Purpose |
|---|---|
| `q8_common.hpp` / `q8_common.cpp` | Shared analytics: `Measurement`/`Stats` structs, `updateStats()`, `mergingWorkerProcessStats()`, Top-K, output formatting. Used unchanged by both implementations. |
| `sequential.cpp` | Single-process entry point. Simulates a 1-master + 8-worker decomposition on one OS process (see `PLAN.md`). |
| `mpi.cpp` | Real MPI entry point. Rank 0 is the master; ranks `1..P-1` are workers. |
| `generate_dataset.py` | Reproducible dataset generator (fixed seed). |
| `run_testcase.sh` | SLURM batch script: builds `mpi_q8` and runs it on one testcase at `P = 9`. |
| `test.sh` | SLURM smoke test: builds both binaries, runs a small inline 10-record input through each, diffs the two outputs. Confirms the cluster's MPI/network setup actually works before running the real benchmarks — not a substitute for `verify_correctness.sh`. |
| `verify_correctness.sh` | One-shot script: builds everything, diffs sequential output against hand-verified expected output, then diffs MPI output against sequential output across multiple process counts and every testcase. |
| `bench_sequential_cluster.sh` | SLURM batch script: benchmarks `sequential` across N = 100 .. 50,000,000, writes timings to `output/sequential_benchmark.log`. |
| `bench_mpi.sh` | SLURM batch script: benchmarks `mpi_q8` across the same N values and `P = 2, 3, 5, 9`, writes timings to `output/mpi_benchmark.log`. |
| `report_q8.md` | The benchmark results write-up: runtime/speedup/efficiency tables and the reasoning behind them (why MPI is usually slower end-to-end, why compute alone scales well, the N=1,000,000 anomaly). |
| `plots/` | Speedup, efficiency, runtime, and time-breakdown plots generated from `output/*.log`. |
| `testcases/` | Self-generated correctness inputs (no official sample I/O exists for Q8 — confirmed absent from the assignment PDF and by TA clarification) plus larger generated datasets (`data_100.txt` .. `data_20M.txt`) used for benchmarking. |
| `testcases/expected/` | Hand-verified expected output for a curated set of testcases, one per required tie-break rule / edge case. |
| `output/` | `sequential_benchmark.log`/`mpi_benchmark.log` from the two bench scripts (tracked in git), plus any per-testcase output from `run_testcase.sh` (not tracked — regenerable). |
| `logs_seq/` | Informal local `time` output from running `sequential` directly on each `testcases/data_*.txt` size — a rough cross-check, not the formal benchmark harness (that's `bench_sequential_cluster.sh`). |
| `Home_Work_2.pdf` | Full assignment PDF (general instructions + Q8 spec). |

## Quick start on the cluster: using the scripts instead of compiling/running by hand

Every script below compiles whatever it needs itself and applies the
right cluster MPI flags — you don't need to run `g++`/`mpicxx`/`mpirun`
manually at all if you just want a result. All are SLURM batch scripts
submitted with `sbatch`; see each script's own section further down for
full details on its input/output.

| Task | Command |
|---|---|
| Run one testcase through MPI | `sbatch run_testcase.sh <testcase-name>` |
| Smoke-test the cluster's build/MPI setup | `sbatch test.sh` |
| Verify sequential + MPI correctness | the `sbatch --wrap=...` command under "Running verify_correctness.sh on the cluster" below (it has no `#SBATCH` header of its own, so it can't be `sbatch`'d directly) |
| Benchmark sequential | `sbatch bench_sequential_cluster.sh` |
| Benchmark MPI | `sbatch bench_mpi.sh` |

## Compilation

```bash
g++ -std=c++17 -O2 sequential.cpp q8_common.cpp -o sequential
mpicxx -std=c++17 -O2 mpi.cpp q8_common.cpp -o mpi_q8
```

**On the cluster**, `mpicxx`/`mpirun` aren't on `PATH` until the OpenMPI
module is loaded — compiling by hand without this first fails with
`mpicxx: command not found`:

```bash
module load openmpi/4.1.5
```

(Every script in this repo already does this `module load` itself before
compiling, so this only matters if you're running the compile command
interactively/by hand.)

## Execution

Input format:

```
N K S
timestamp station_id temperature humidity pressure rainfall wind_speed
... (N lines)
```

`N` = number of measurements, `K` = number of Top-K stations to report,
`S` = number of distinct stations (`station_id` in `[0, S-1]`).

```bash
./sequential < input.txt > output.txt
mpirun -np <P> ./mpi_q8 < input.txt > output.txt
```

`P` is the **total** number of MPI processes (rank 0 = master, ranks
`1..P-1` = workers, so `P` must be at least 2). Only rank 0 writes the
required output to stdout. Check the result with:

```bash
cat output.txt
```

### Running a testcase on the cluster (`run_testcase.sh`)

```bash
sbatch run_testcase.sh <testcase-name>
```

`<testcase-name>` selects the input file `testcases/<testcase-name>.txt`
(name only, no `.txt`); it defaults to `test_sample_input` if omitted. The
script always runs at `P = 9` (`--ntasks=9`). Output is written to
`output/<testcase-name>.txt`; SLURM/compile/mpirun logs go to `%j.log`/
`%j.err` instead, so `output/` always holds just the program's required
stdout. Check the result with:

```bash
cat output/<testcase-name>.txt
```

### Smoke-testing the cluster setup (`test.sh`)

```bash
sbatch test.sh
```

Builds both binaries, generates a small 10-record input inline (not read
from `testcases/`, so this doesn't depend on what's synced to the
cluster), runs it through `./sequential` (writing `seq_output.txt`) and
`mpirun -np 9 ./mpi_q8` with the cluster's UCX/vader MCA workaround flags
(writing `mpi_output.txt`), then diffs the two. This just confirms the
cluster's compiler/MPI/network stack works end-to-end — the real
correctness check is `verify_correctness.sh`. Check the result in
`%j.log`, which prints `PASS: MPI output matches sequential output` or
`FAIL` with the diff.

## Dataset generation

```bash
python3 generate_dataset.py --n 1000 --k 5 --s 20 --seed 42 --out data.txt
```

Fixed seed (`42` by default) makes generation reproducible. Value ranges
and other generation assumptions are documented in the script's header
comment. Check the output with:

```bash
head -1 data.txt   # should read "1000 5 20"
wc -l data.txt      # should be 1001 (header + N data lines)
```

## Correctness verification

No official sample input/output exists for Q8 (checked against the
assignment PDF directly — Q4–Q6 have worked samples, Q7/Q8 do not — and
confirmed by the TA clarification thread). Correctness is instead
established two ways:

1. **Expected-output checks** — `testcases/expected/` holds hand-verified
   expected output for one input file per required tie-break rule / edge
   case (Top-K tie, hottest tie, coldest tie, busiest-interval tie,
   extreme-temperature boundary, a station's records split across
   multiple worker partitions, `K > S`, `N = 0`, plus a general sample).
   `sequential`'s actual output is diffed against these byte-for-byte.
2. **Differential testing** — with sequential now verified, it becomes
   the correctness oracle for MPI: `mpi_q8`'s output is diffed against
   `sequential`'s output across every testcase in `testcases/` (including
   `N < numWorkers` and `N` not divisible by `numWorkers`), at multiple
   process counts.

Run everything with:

```bash
./verify_correctness.sh
```

This builds `sequential` and `mpi_q8`, runs both checks above at
`P = 2, 4, 9`, and reports a final pass/fail summary (non-zero exit on
any failure) directly to the terminal — that printed `PASS`/`FAIL` per
testcase plus the final `VERIFICATION: PASSED`/`FAILED` line **is** the
output to check; there's no separate output file for a local run.

### Running verify_correctness.sh on the cluster

`verify_correctness.sh` has no `#SBATCH` header of its own and calls
`mpirun` directly, so it needs `module load` plus the same MCA workaround
flags `test.sh`/`run_testcase.sh` use on this cluster (UCX isn't
installed; vader's default CMA path fails with a `ptrace_scope` error).
Submit it as a batch job, exporting those flags as `OMPI_MCA_*`
environment variables so every `mpirun` call inside the script picks them
up without editing it:

```bash
sbatch --job-name=q8-verify --ntasks=9 --nodes=1 --mem-per-cpu=4G --time=00:30:00 \
  --output=%j.log --error=%j.err \
  --wrap="module load openmpi/4.1.5 && export OMPI_MCA_pml=ob1 OMPI_MCA_osc=^ucx OMPI_MCA_btl=vader,self OMPI_MCA_btl_vader_single_copy_mechanism=none && ./verify_correctness.sh"
```

Check the result in `%j.log` (the script's PASS/FAIL lines and final
`VERIFICATION: PASSED`/`FAILED` summary go to stdout, which SLURM
redirects there) — `%j.err` only carries SLURM/module noise.

### Known limitation: floating-point rounding at large N

`sequential`'s summation isn't a single straight linear pass — it calls
`runMasterWorkerSimulation()` with a fixed 8 logical workers, so it sums
each of 8 partitions separately and merges them, same as the real MPI
program does for actual workers. Floating-point addition isn't
associative, so summing the same values in a different number of chunks
(e.g. 1 chunk at real `P=2`, vs `sequential`'s fixed 8 chunks) can round
differently in the last decimal place after millions of additions.

In practice this is invisible at the small `testcases/` sizes
`verify_correctness.sh` uses, and `P=9` (8 real workers) always matches
`sequential` exactly, since the partition shape is identical. But at
large N (observed at N=2,000,000) sums like `TOTAL_RAINFALL` can differ
from `sequential`'s output by 1 in the last printed decimal digit at
some other `P` values (e.g. `P=2`, `P=3`) — not a correctness bug, just
expected floating-point non-associativity, and consistent with the
assignment's own note that "where floating-point accumulation order can
affect the final rounded value, use a deterministic strategy." Not
fixed here (would require compensated/Kahan summation or an
order-independent reduction); documented as a known limitation instead.

## Architecture notes

- `P` always means the *total* number of MPI processes. Rank 0 is a
  non-computing master (reads input, partitions it, distributes it,
  merges worker results, prints output); ranks `1..P-1` are workers
  (`numWorkers = P - 1`).
- Records are distributed with `MPI_Scatterv` using a balanced
  contiguous partition (same rule the sequential simulation uses).
- Each worker's local `Stats` is sent back to the master as plain
  primitive arrays (grouped by C++ type) over a handful of `MPI_Send`
  calls — not a derived MPI datatype — since at most a few workers each
  send one aggregated result, so the message count is negligible next to
  the cost of processing the underlying measurements. The `Measurement`
  datatype used for `MPI_Scatterv` *is* a derived MPI datatype, since
  millions of individual records cross that wire.
- The master reconstructs each worker's `Stats` from the received
  arrays and merges it with the same `mergingWorkerProcessStats()` the
  sequential version uses — no separate MPI-side merge logic.

## Benchmarking

Two SLURM batch scripts benchmark the two implementations on identical
hardware and identical data (same `K=10 S=100 seed=42`, same seven input
sizes N = 100, 1,000, 10,000, 100,000, 1,000,000, 20,000,000,
50,000,000), so their timings are directly comparable for
speedup/efficiency.

### Sequential (`bench_sequential_cluster.sh`)

```bash
sbatch bench_sequential_cluster.sh
```

For each size: generates the input with `generate_dataset.py`, times
`./sequential < input > /dev/null` with the shell's `time`, then deletes
the input before moving to the next size (so disk usage stays bounded at
large N). Results accumulate in `output/sequential_benchmark.log`, one
`N=... K=... S=... seed=...` header line followed by a `real`/`user`/
`sys` block per size. `%j.log` mirrors the same progress echoes;
`%j.err` carries only SLURM/compile stderr.

### MPI (`bench_mpi.sh`)

```bash
sbatch bench_mpi.sh
```

Same structure, but for each size it also loops over `P = 2, 3, 5, 9`
(i.e. 1, 2, 4, 8 workers, doubling worker counts each step), running
`mpirun -np $P` with the cluster's UCX/vader MCA workaround flags each
time, timed the same way. Results accumulate in
`output/mpi_benchmark.log`, one `N=...` block containing a `--- P=... ---`
sub-block per process count.

### Checking the results

```bash
cat output/sequential_benchmark.log
cat output/mpi_benchmark.log
```

Each block's `real` line is the wall-clock time to use for speedup
(`T_sequential(N) / T_mpi(N, P)`) and efficiency
(`speedup / (P - 1)`, since only `P - 1` ranks actually compute).

### Results, plots, and analysis

Both logs are complete for all 7 sizes and all 4 process counts. The
full write-up — runtime/speedup/efficiency tables, why MPI is usually
*slower* than sequential end-to-end (a serial, rank-0-only input read is
the bottleneck), why the actual counting logic scales almost perfectly
once that's excluded, and the reasoning behind both — is in
[`report_q8.md`](report_q8.md). Speedup/efficiency/breakdown
plots are in [`plots/`](plots/).

Headline result: the MPI implementation is correct and its compute
phase scales close to linearly (~8x on 8 workers, once I/O is excluded),
but end-to-end it only beats sequential at the two largest sizes
(20M/50M records) and only with 4 or fewer workers — using all 8 workers
is the slowest configuration at every size tested, because only rank 0
reads the input and that step gets *worse*, not just non-parallel, as
more workers are added. See `report_q8.md` for the full reasoning.
