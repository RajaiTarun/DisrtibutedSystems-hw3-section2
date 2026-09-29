# Q1: Weather Analytics (HW2 Q8) with MapReduce

This folder solves HW2 Q8 (weather and environmental data analytics) with the
**MapReduce** model. The input format, output format and tie-break rules are the same as HW2.
Three C++ programs (`mapper`, `combiner`, `reducer`) read **stdin** and write **stdout**, so they
are **Hadoop Streaming** programs. They are run with the Slurm script we were told to use
while Hadoop on RCE is unavailable (see [Section 10](#10-running-on-hadoop-when-available)).

This README explains how to run **everything from zero**: setup, build, local run,
correctness check, cluster runs, benchmarks, plots, and cleanup.
The design and the analysis of the results are in the report (`../report/report.tex`).

---

## Contents
1. [Folder layout](#1-folder-layout)
2. [Input and output format](#2-input-and-output-format)
3. [Setup (Mac / Linux laptop)](#3-setup-mac--linux-laptop)
4. [Build and run locally](#4-build-and-run-locally)
5. [Correctness check locally](#5-correctness-check-locally)
6. [Setup on the RCE cluster](#6-setup-on-the-rce-cluster)
7. [Run one job on the cluster](#7-run-one-job-on-the-cluster-run_slurmsh)
8. [Benchmarks on the cluster](#8-benchmarks-on-the-cluster)
9. [Copy the results back and make the plots](#9-copy-the-results-back-and-make-the-plots)
10. [Running on Hadoop (when available)](#10-running-on-hadoop-when-available)
11. [Cleanup](#11-cleanup)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Folder layout

The repository has two folders next to each other. `q1_mapreduce` uses `../q8`, so **keep both**.

```
ds-hw3-sec2/
├── q1_mapreduce/                  <- this folder (HW3 Section 2, Q1)
│   ├── mapper.cpp                 map: 1 input record -> key/value lines
│   ├── combiner.cpp               combine: merges lines with the same key (same format in and out)
│   ├── reducer.cpp                reduce: merges everything and prints the final Q8 output
│   ├── mr_common.hpp              shared code: parse a line, merge two values, tie-break rules
│   ├── run_local.sh               run the pipeline on one file on your laptop
│   ├── verify_correctness.sh      all correctness tests on your laptop (69 tests)
│   ├── compare_outputs.py         compares two outputs (EXACT / FP_CLOSE / FAIL)
│   ├── run_slurm.sh               one run on the cluster (Slurm), with per-stage timing
│   ├── bench_slurm.sh             main benchmark: sequential vs MPI vs MapReduce
│   ├── bench_extra.sh             extra benchmark: memory, separate stages, multiple nodes
│   ├── check_parallel.sh          proves the map tasks run in parallel (and on which nodes)
│   ├── plot_results.py            makes plots/*.png and results/summary.md
│   ├── results/                   benchmark results (CSV + logs) used in the report
│   ├── plots/                     the plots used in the report
│   └── tutorial.md                beginner explanation of MapReduce and of this code
└── q8/                            HW2 Q8 code (used as the reference and for the MPI comparison)
    ├── sequential.cpp, mpi.cpp, q8_common.cpp/.hpp
    ├── generate_dataset.py        reproducible dataset generator (fixed seed)
    └── testcases/                 HW2 test inputs + expected outputs
```

---

## 2. Input and output format

Input (same as HW2 Q8):
```
N K S
timestamp station_id temperature humidity pressure rainfall wind_speed     (N lines)
```

Output (same as HW2 Q8; every decimal number has exactly 6 digits after the point):
```
TOTAL_MEASUREMENTS <value>
AVERAGE_TEMPERATURE <value>
... (min/max/averages, rainfall, wind, extreme events)
HOTTEST_MEASUREMENT <temperature> <station_id> <timestamp>
COLDEST_MEASUREMENT <temperature> <station_id> <timestamp>
BUSIEST_INTERVAL <interval_id> <count>
TOP_STATIONS
<station_id> <count> <average_temperature> <total_rainfall>      (K lines)
```

Intermediate format (between mapper, combiner and reducer): one `key<TAB>values` line.

| Key | Value | Used for |
|---|---|---|
| `K` | K from the header | size of the top-K list |
| `G` | 21 numbers: count, sums, mins, maxes, extreme count, hottest, coldest | all global statistics |
| `S<station>` | `count temp_sum rain_sum` | TOP_STATIONS |
| `I<interval>` | `count` (interval = timestamp / 60) | BUSIEST_INTERVAL |

---

## 3. Setup (Mac / Linux laptop)

You need a C++17 compiler, Python 3 and (only for plots) pandas + matplotlib.

**Mac:**
```bash
xcode-select --install          # installs g++ (clang) and make; skip if already installed
g++ --version                   # check
python3 --version               # check (3.8 or newer)
pip3 install pandas matplotlib  # only needed for plot_results.py
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt install g++ python3 python3-pip
pip3 install pandas matplotlib
```

**Get the code:**
```bash
git clone https://github.com/RajaiTarun/DisrtibutedSystems-hw3-section2.git ds-hw3-sec2
cd ds-hw3-sec2/q1_mapreduce
```

---

## 4. Build and run locally

`run_local.sh` compiles the three programs and runs the whole pipeline on one input file:
```
mapper < input | LC_ALL=C sort | combiner | LC_ALL=C sort | reducer
```

```bash
cd ds-hw3-sec2/q1_mapreduce
chmod +x run_local.sh verify_correctness.sh      # only needed once
./run_local.sh ../q8/testcases/test_sample_input.txt              # prints the result
./run_local.sh ../q8/testcases/test_sample_input.txt out.txt      # saves it to out.txt
```

Compile by hand (what the scripts do):
```bash
g++ -std=c++17 -O2 -Wall mapper.cpp   -o mapper
g++ -std=c++17 -O2 -Wall combiner.cpp -o combiner
g++ -std=c++17 -O2 -Wall reducer.cpp  -o reducer
```

Run on a bigger generated dataset (same generator and seed as HW2):
```bash
python3 ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out data_1M.txt
./run_local.sh data_1M.txt
```

See each stage's output (useful to understand the design, see `tutorial.md`):
```bash
./mapper < ../q8/testcases/test_sample_input.txt | head                     # key/value lines
./mapper < ../q8/testcases/test_sample_input.txt | LC_ALL=C sort | ./combiner  # merged per key
```

> Always use `LC_ALL=C sort`. With other language settings `sort` may ignore the tab
> and mix up keys like `S1` and `S10`, which breaks the grouping.

---

## 5. Correctness check locally

```bash
./verify_correctness.sh
```
It builds everything (including HW2's `sequential` from `../q8`) and runs:

* **Test 1**: the 9 HW2 test cases that have hand-checked expected outputs (ties, extreme
  boundaries, K > S, N = 0, ...): the MapReduce output must be identical.
* **Test 2**: all 13 HW2 test cases + generated 100K and 1M datasets, each split into
  P = 1, 2, 4, 8 chunks (one mapper per chunk, like on the cluster). Output must be identical to
  `sequential`.

Expected last lines:
```
=== Summary: 69 passed, 0 failed ===
VERIFICATION: PASSED
```

Compare any two output files yourself:
```bash
python3 compare_outputs.py expected.txt actual.txt     # prints EXACT, FP_CLOSE or FAIL
```
`FP_CLOSE` means the only differences are floating point rounding in the last digits of very
large sums (e.g. `TOTAL_RAINFALL` at 10M+ records), caused by adding the numbers in a different
order. Counts, ids, timestamps and the top-K order must always match exactly. The report explains
why.

---

## 6. Setup on the RCE cluster

**1. Log in** (from your laptop):
```bash
ssh <username>@rce.iiit.ac.in
```

**2. Clone the repository** in your home folder:
```bash
cd ~
git clone https://github.com/RajaiTarun/DisrtibutedSystems-hw3-section2.git ds-hw3-sec2
cd ds-hw3-sec2/q1_mapreduce
```
(If the repository is private, git asks for a username and a GitHub **personal access token**
as the password.) If `git clone` does not work on the cluster, copy the folder from your laptop:
```bash
scp -r ds-hw3-sec2 <username>@rce.iiit.ac.in:~/          # run this on the laptop
```

**3. Check the tools** (all used by the scripts):
```bash
g++ --version                               # C++ compiler
python3 --version                           # dataset generator + compare_outputs.py
module load openmpi/4.1.5 && mpicxx --version   # only for the MPI comparison
/usr/bin/time -f "%e %M" ls > /dev/null     # only for bench_extra.sh (memory); must not error
```

**4. Check your disk quota** (the 50M-record runs need ~5 GB free):
```bash
quota -s
```

The scripts compile the programs themselves on the cluster. Binaries built on a Mac do not run
on Linux.

---

## 7. Run one job on the cluster (`run_slurm.sh`)

`run_slurm.sh` follows the course's `Mapreduce_distributed.sh`:
```
split input into P chunks                 (P = number of Slurm tasks = number of mappers)
srun P tasks:  mapper < chunk_XX > map_XX.out
srun P tasks:  LC_ALL=C sort map_XX.out > sort_XX.out
srun P tasks:  combiner < sort_XX.out > comb_XX.out
master:        LC_ALL=C sort -m comb_*.out > shuffled.out        (shuffle)
master:        reducer < shuffled.out > output/mr_<input name>
```
It then runs HW2's `sequential` on the same input and compares the two outputs.

```bash
cd ~/ds-hw3-sec2/q1_mapreduce
sbatch run_slurm.sh                                                   # sample input, 4 mappers
sbatch --ntasks=8 run_slurm.sh ../q8/testcases/test_topk_tie.txt      # 8 mappers
sbatch --ntasks=4 --nodes=2 run_slurm.sh ../q8/testcases/test_station_split.txt   # 2 nodes

python3 ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out data_1M.txt
sbatch --ntasks=4 run_slurm.sh data_1M.txt                            # a bigger file
```

Check the job:
```bash
squeue -u $USER                 # the job is listed while it waits (PD) or runs (R)
cat <jobid>.log                 # stage times, line counts, CORRECTNESS line
cat <jobid>.err                 # errors, if any (should be empty)
cat output/mr_data_1M.txt       # the MapReduce output
grep -E "Nodes|CORRECTNESS" *.log
```
Expected: `CORRECTNESS: PASSED (EXACT, compared with sequential)`.

Run HW2's sequential program by itself (same input, to compare by hand):
```bash
srun --ntasks=1 ./sequential < data_1M.txt > output/seq_data_1M.txt
python3 compare_outputs.py output/seq_data_1M.txt output/mr_data_1M.txt
```
(`run_slurm.sh` already compiled `./sequential`. To build it yourself:
`g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o sequential`.)

> Keep large runs on compute nodes (`sbatch` / `srun`), not on the login node.
> `run_slurm.sh` writes the full mapper output to disk, so use it for inputs up to ~10M records;
> the benchmark scripts below use pipes and handle 50M.

---

## 8. Benchmarks on the cluster

Only run **one benchmark job at a time** (they share the node and your disk quota).

### 8.1 Main benchmark (`bench_slurm.sh`, ~1 hour)
For N = 1M, 10M, 20M, 50M records (K=10, S=100, seed 42, the same data as HW2) it runs, on one
node with 9 Slurm tasks:
* HW2 `sequential` (the reference output and baseline time),
* HW2 `mpi` with 1, 2, 4, 8 workers (`mpirun -np workers+1`, same flags as HW2),
* MapReduce with 1, 2, 4, 8 mappers (map phase = `mapper | sort | combiner` as one pipe per task),
* MapReduce **without** the combiner for 1M and 10M.

Every run is compared with `sequential`.

```bash
cd ~/ds-hw3-sec2/q1_mapreduce
BENCH_SIZES="10000 100000" sbatch bench_slurm.sh      # optional quick test (1-2 min)
sbatch bench_slurm.sh                                 # full benchmark
tail -f bench_<jobid>.log                             # Ctrl+C stops watching, not the job
grep -c FAIL results/bench_results.csv                # must print 0
```
Output: `results/bench_results.csv` with columns
`impl,N,workers,total_s,split_s,map_s,shuffle_s,reduce_s,shuffle_lines,correct`.

### 8.2 Extra measurements (`bench_extra.sh`)
```bash
sbatch bench_extra.sh                                          # memory + stages (~20 min, 1 node)
# wait until it has finished, then:
PART=multinode sbatch --nodes=4 --ntasks=8 bench_extra.sh      # 8 mappers on 4 nodes (~6 min)
```
* `PART=memory`: peak memory (`/usr/bin/time -f "%e %M"`) of every process at 50M records,
  4 and 8 workers -> `results/extra_memory.csv`
* `PART=stages`: 10M records, 4 mappers, mapper / sort / combiner run as separate stages
  -> `results/extra_stages.csv`, `results/extra_stages_mapper_bytes.txt`
* `PART=multinode`: 50M records, MapReduce spread over several nodes
  -> `results/extra_multinode.csv`

If 4 nodes are not available (job stays `PD`), use `--nodes=2 --ntasks=8`.

### 8.3 Parallelism check (`check_parallel.sh`, ~2 min)
Runs the map phase (`mapper | sort | combiner`) on 10M records in 8 pieces with `srun`; every task
records its node, CPU core, start and end time. Then it runs the same 8 pieces one after another.
```bash
sbatch check_parallel.sh                 # 8 tasks on 1 node
sbatch --nodes=4 check_parallel.sh       # 8 tasks spread over 4 nodes
cat parallel_<jobid>.log
```
In parallel, all tasks start at the same time, run on different cores (and on different nodes with
`--nodes=4`), and "average tasks running at the same time" is close to 8. Our logs are in
`results/check_parallel_1node.log` and `results/check_parallel_4nodes.log`.

---

## 9. Copy the results back and make the plots

On the **laptop** (replace `<username>` and the job ids):
```bash
cd ds-hw3-sec2/q1_mapreduce
mkdir -p results
scp "<username>@rce.iiit.ac.in:~/ds-hw3-sec2/q1_mapreduce/results/*.csv" results/
scp "<username>@rce.iiit.ac.in:~/ds-hw3-sec2/q1_mapreduce/results/extra_stages_mapper_bytes.txt" results/
scp <username>@rce.iiit.ac.in:~/ds-hw3-sec2/q1_mapreduce/bench_<jobid>.log results/bench_full.log
```

Make the plots and the summary tables:
```bash
python3 plot_results.py
```
Output:

| File | Content |
|---|---|
| `plots/runtime_vs_workers.png` | runtime vs workers, one panel per input size |
| `plots/runtime_vs_size.png` | runtime and throughput vs input size (4 workers) |
| `plots/speedup.png` | speedup of MapReduce and MPI vs their own 1-worker run |
| `plots/mr_stage_breakdown.png` | split / map / shuffle / reduce time at 50M |
| `plots/combiner_effect.png` | with vs without the combiner at 10M |
| `plots/shuffle_lines.png` | lines reaching the shuffle vs number of mappers |
| `plots/memory.png` | peak memory per process at 50M |
| `plots/map_phase_stages.png` | mapper / sort / combiner run separately at 10M |
| `plots/multinode.png` | 1 node vs 4 nodes at 50M |
| `results/summary.md` | all numbers as markdown tables |

---

## 10. Running on Hadoop (when available)

Hadoop is **not installed on RCE** (no `hadoop` module; only `spark/3.0.1-hadoop-3.2`), and
the course said to use a Slurm script until it is fixed. The three programs follow the Hadoop
Streaming rules (stdin/stdout, `key<TAB>value`, combiner output = combiner input format), so on
a working Hadoop 3.3.6 installation they run unchanged:

```bash
g++ -std=c++17 -O2 mapper.cpp -o mapper          # compile on a machine of the Hadoop cluster
g++ -std=c++17 -O2 combiner.cpp -o combiner
g++ -std=c++17 -O2 reducer.cpp -o reducer

hdfs dfs -mkdir -p /q8 && hdfs dfs -put data_1M.txt /q8/
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files mapper,combiner,reducer \
    -mapper ./mapper -combiner ./combiner -reducer ./reducer \
    -numReduceTasks 1 \
    -input /q8/data_1M.txt -output /q8/out
hdfs dfs -cat /q8/out/part-00000
```
**`-numReduceTasks 1` is required**: top-K and the busiest interval need every station and every
interval in one reducer.

Without HDFS/YARN (Hadoop "local mode"), add `-D mapreduce.framework.name=local -fs file:///`
after `hadoop jar <jar>` and use normal file paths.

---

## 11. Cleanup

On the cluster (only when **no job is running**, check `squeue -u $USER` first):
```bash
cd ~/ds-hw3-sec2/q1_mapreduce
rm -rf work_* bench_work_* extra_work_* check_work_* output data_*.txt
rm -f *.err
quota -s
```
The compiled programs (`mapper`, `combiner`, `reducer`, `sequential`, `mpi_q8`) are ignored by
git and rebuilt by every script.

---

## 12. Troubleshooting

| Problem | Cause / fix |
|---|---|
| `split: l/2: number of chunks is invalid` on a Mac | macOS `split` has no `-n l/P`. Only the Slurm scripts use it (Linux). Locally use `verify_correctness.sh`, which splits by line count. |
| Wrong totals, a key appears twice after the combiner | `sort` was run without `LC_ALL=C`. |
| `FP_CLOSE` instead of `EXACT` | Correct result: only rounding in the last digits of very large sums (see Section 5). |
| MPI with 1/2/4 workers is `FP_CLOSE` too | Same reason: HW2 `sequential` adds in the order of an 8-way split, so only 8 workers match bit for bit. |
| Job stays `PD` | Waiting for free nodes; `squeue -u $USER` shows the reason. Use fewer nodes. |
| `Disk quota exceeded` | Delete old `bench_work_*`, `extra_work_*`, `data_*.txt`; run one benchmark at a time. |
| `git pull` refuses: untracked files would be overwritten | The cluster has an untracked copy of `results/`. Delete it (`rm -rf results`) and pull again. |
| `/usr/bin/time: invalid option -- 'f'` | GNU time is missing; `bench_extra.sh` needs it (the other scripts do not). |
| `module: command not found` in a local test | Harmless; only the cluster has environment modules. |
