# Q2: Real-Time Weather Analytics (HW2 Q8) with gRPC in Python

This folder solves HW2 Q8 (weather and environmental data analytics) as a **live stream**, in
**Python with `grpcio`**. A client replays the dataset through gRPC. A coordinator spreads the records
over several worker processes, and CLI dashboards show the current analytics while the data is still
arriving. The final analytics are exactly the HW2 output (same format, same tie-break rules).

This README explains how to run **everything from zero**: setup, local run, correctness, cluster demo,
benchmarks, plots, and cleanup. `tutorial.md` explains gRPC and the design for beginners, and
`implementation_plan.md` is the plan we followed.

---

## Contents
1. [Architecture](#1-architecture)
2. [Folder layout](#2-folder-layout)
3. [The gRPC interface](#3-the-grpc-interface-weatherproto)
4. [Setup on a Mac / Linux laptop](#4-setup-on-a-mac--linux-laptop)
5. [Run locally](#5-run-locally)
6. [Correctness check](#6-correctness-check)
7. [Setup on the RCE cluster](#7-setup-on-the-rce-cluster)
8. [Live demo on the cluster](#8-live-demo-on-the-cluster-rce-guide-style)
9. [Benchmarks](#9-benchmarks)
10. [Plots](#10-plots)
11. [Program reference](#11-program-reference)
12. [Cleanup](#12-cleanup)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Architecture

```
 client.py ──Configure + stream of RecordBatch──► coordinator.py ──ProcessBatch──► worker.py 0 ┐
 (replays the file:                                (gRPC server)  ──ProcessBatch──► worker.py 1 ├─ each keeps
  batch size, rate)                                     ▲         ──ProcessBatch──► worker.py W ┘  a Stats object
                                                        │ GetAnalytics()                          (+ lock)
 dashboard.py (live / --once / --bench) ────────────────┘  the coordinator asks every worker for its
                                                            partial Stats (GetPartial) and merges them
```

| Program | Role |
|---|---|
| `client.py` | reads the dataset and builds the gRPC messages, sends the header (`Configure`), then streams the records in batches over one gRPC client stream, at a chosen rate |
| `coordinator.py` | gRPC server. Splits every batch between the workers (one bounded queue + one sender thread per worker, so all workers work in parallel), and answers queries by asking all workers at the same time and merging their partial results |
| `worker.py` | gRPC server (one process per worker). Keeps a `Stats` object for its records, protected by a lock |
| `dashboard.py` | gRPC client: live CLI view, `--once` (final result in HW2 format), `--bench` (query latency), `--stats`, `--shutdown` |
| `weather_stats.py` | the Q8 analytics in Python (a port of HW2's `q8_common.cpp`): `Stats`, `update`, `merge`, top-K, busiest interval, HW2 output format |
| `sequential.py` | the analytics without gRPC, used to check `weather_stats.py` against HW2's C++ `sequential` |

**Which worker gets a record (strategy):**
* `interval` (default): `worker = (timestamp // 60) % W`. Every 60-second interval lives on exactly one
  worker, so a worker only reports its own busiest interval, and the coordinator picks the best one.
  Queries stay small and fast, however much data has arrived.
* `roundrobin`: records go to the workers in turn. Perfectly even load, but an interval is spread over
  all workers, so each query must ship every worker's whole interval map (slower queries).

**Why one process per worker:** in Python, only one thread of a process runs Python code at a time
(the GIL). Separate processes do run at the same time, so W worker processes give real parallelism.

---

## 2. Folder layout

```
q2_grpc/
├── weather.proto            gRPC interface (messages + services)
├── weather_pb2.py / weather_pb2_grpc.py   code generated from weather.proto (committed)
├── gen_proto.sh             regenerates them after weather.proto changes (laptop, needs grpcio-tools)
├── requirements.txt         what is needed to run: grpcio, protobuf (the only packages on the cluster)
├── requirements-laptop.txt  + grpcio-tools (code generation) and pandas, matplotlib (plots), laptop only
├── setup_env.sh             creates the virtual environment .venv
├── weather_stats.py         Q8 analytics (Stats), port of HW2's q8_common.cpp
├── sequential.py            analytics without gRPC (test of weather_stats.py)
├── grpc_common.py           channel helper (no message size limit, waits for the server)
├── worker.py  coordinator.py  client.py  dashboard.py
├── run_local.sh             whole system on one machine, with the live dashboard
├── verify_correctness.sh    253 correctness tests against HW2's sequential program
├── run_cluster.sh           live demo on the cluster (RCE guide style)
├── bench.sh                 benchmarks (cluster with sbatch, or one machine)
├── measure_resources.sh     peak memory + cpu time of every process of one run (uses run_measured.py)
├── plot_results.py          plots/*.png and results/summary.md from the benchmark CSV
├── results/  plots/         benchmark results and plots used in the report
├── implementation_plan.md   the plan
├── tutorial.md              beginner explanation of gRPC and of this design
└── README.md                this file
```
It also uses `../q8/` (`sequential.cpp` as the reference, `generate_dataset.py`, test cases) and
`../q1_mapreduce/compare_outputs.py`, so keep the whole repository.
`.venv/` is created on each machine and not committed. The generated `weather_pb2.py` /
`weather_pb2_grpc.py` are committed, so the cluster does not need the code generator.

---

## 3. The gRPC interface (`weather.proto`)

```
service Coordinator {
  rpc Configure(StreamConfig) returns (Ack);                    // header N K S, resets the analytics
  rpc StreamRecords(stream RecordBatch) returns (Ack);          // client stream; returns when all is processed
  rpc GetAnalytics(AnalyticsRequest) returns (AnalyticsReply);  // current analytics + live counters
  rpc Shutdown(Empty) returns (Ack);
}
service Worker {
  rpc Configure(WorkerConfig) returns (Ack);
  rpc ProcessBatch(RecordBatch) returns (Ack);
  rpc GetPartial(Empty) returns (PartialStats);
  rpc Shutdown(Empty) returns (Ack);
}
```
* `RecordBatch` holds many `Record`s, so the client chooses how many records go in one message.
* `StreamRecords` is a **client-streaming** RPC: one long connection for the whole dataset.
* `AnalyticsReply.result` is the analytics in exactly the HW2 text format, plus live counters
  (records received / processed, elapsed time, ingest rate, records per worker, done flag).
* `PartialStats` has the same fields as HW2's `Stats`; for `interval` it carries only the worker's
  busiest interval, for `roundrobin` all of its intervals.

---

## 4. Setup on a Mac / Linux laptop

grpcio needs **Python 3.9 or newer**. `setup_env.sh` creates a virtual environment `.venv` in this
folder (only Python packages, nothing installed on the system). On the laptop it also installs the
code generator and the plotting packages:

```bash
git clone https://github.com/RajaiTarun/DisrtibutedSystems-hw3-section2.git ds-hw3-sec2
cd ds-hw3-sec2/q2_grpc
./setup_env.sh --laptop                          # picks the newest python (3.13, 3.12, ..., 3.9)
PYTHON=python3.12 ./setup_env.sh --laptop        # or choose one yourself
```
Mac: the built-in `/usr/bin/python3` is old (3.9); `brew install python` gives a newer one.
If `pip` hangs while downloading, stop it (Ctrl+C) and run `./setup_env.sh` again.
You also need `g++` (for HW2's `sequential`, used as the reference): `xcode-select --install` on a Mac.

All scripts use `.venv/bin/python` directly. To run the programs by hand, first:
```bash
source .venv/bin/activate               # in every new terminal
```

---

## 5. Run locally

**Everything at once, with the live dashboard** (`run_local.sh <input> [workers] [strategy] [batch] [rate]`):
```bash
.venv/bin/python ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out data_1M.txt
./run_local.sh data_1M.txt 4 interval 1000 100000     # 100,000 records/s, so you can watch it grow
./run_local.sh ../q8/testcases/test_sample_input.txt  # a small HW2 test case
```
The dashboard refreshes every 300 ms until the stream is done. Then the script prints the client's
summary and compares the final result with HW2's sequential program (`EXACT` or `FP_CLOSE`).
Logs and the final output are in `logs/`.

**Or start every part yourself, each in its own terminal** (after `source .venv/bin/activate`):
```bash
python worker.py 127.0.0.1:50061                                    # terminal 1
python worker.py 127.0.0.1:50062                                    # terminal 2
python coordinator.py 127.0.0.1:50051 interval 127.0.0.1:50061 127.0.0.1:50062   # terminal 3
python dashboard.py 127.0.0.1:50051                                 # terminal 4: live view (Ctrl+C quits)
python dashboard.py 127.0.0.1:50051                                 # terminal 5: a second dashboard
python client.py 127.0.0.1:50051 data_1M.txt 1000 100000            # terminal 6: stream at 100,000 rec/s
python dashboard.py 127.0.0.1:50051 --once                          # final result (HW2 format)
python dashboard.py 127.0.0.1:50051 --shutdown                      # stops coordinator + workers
```
The client can be run again at any time: every stream starts with `Configure`, which resets the analytics.

---

## 6. Correctness check

```bash
./verify_correctness.sh          # about 3 minutes
```
Every final result is compared with HW2's C++ `sequential` using `../q1_mapreduce/compare_outputs.py`:

* **Test 0:** `sequential.py` (our Python analytics, no gRPC) on all 13 HW2 test cases (13 runs).
* **Test 1:** all 13 HW2 test cases (ties, extreme values, K > S, N = 0, empty file, ...) through the
  gRPC system with 1, 2 and 4 workers, both strategies, and batch sizes 1, 7 and 1000 (234 runs).
* **Test 2:** generated 100K and 1M datasets (seed 42), 4 workers, both strategies (4 runs).
* **Test 3:** 4 dashboards query non-stop while 1M records stream in: no query may fail, the processed
  count may never go down between two queries and never exceed N, and the final result must be correct.

Expected end:
```
=== Summary: 253 passed, 0 failed ===
VERIFICATION: PASSED
```
`FP_CLOSE` means the only differences are floating point rounding: the last digits of very large sums
(adding in a different order), or one unit in the 6th decimal when the exact value lies exactly halfway
(a rounding tie). Counts, ids, timestamps and the top-K order always match exactly. See the report.

---

## 7. Setup on the RCE cluster

The cluster's default `python3` is **3.6**, too old for grpcio. Use `/usr/bin/python3.11` (or the
`python/3.12.5` module):
```bash
ssh <username>@rce.iiit.ac.in
cd ~ && git clone https://github.com/RajaiTarun/DisrtibutedSystems-hw3-section2.git ds-hw3-sec2
cd ~/ds-hw3-sec2/q2_grpc
PYTHON=/usr/bin/python3.11 ./setup_env.sh
```
**What this installs on the cluster:** only `grpcio` and `protobuf` (plus `typing_extensions`, which
grpcio needs), into `.venv` in this folder. Nothing system-wide; `rm -rf .venv` removes it all.
If the chosen Python already has grpcio and a recent enough protobuf, **nothing** is installed.
Plots are made on the laptop, so pandas / matplotlib are never needed on the cluster.

The virtual environment lives in your home folder, which every compute node sees, so the programs run
on any node with `.venv/bin/python`.

---

## 8. Live demo on the cluster (RCE guide style)

Coordinator and workers on compute nodes, and the client and two dashboards on **other** compute nodes,
each in its own terminal:
```bash
cd ~/ds-hw3-sec2/q2_grpc
salloc --nodes=4 --ntasks-per-node=1 --time=01:00:00
./run_cluster.sh start 4 interval
```
The script starts the coordinator on the first node and 4 workers on the other nodes, then prints the
exact commands for the other terminals, for example:
```
  # dashboard 1
  ssh node03
  cd ~/ds-hw3-sec2/q2_grpc && .venv/bin/python dashboard.py node01:50110
  # dashboard 2
  ssh node04
  ...
  # streaming client
  ssh node02
  cd ~/ds-hw3-sec2/q2_grpc && .venv/bin/python client.py node01:50110 data_1M.txt 1000 100000
```
Open a new terminal for each, log in to rce, and run them. Both dashboards update while the client
streams. At the end:
```bash
.venv/bin/python dashboard.py <coordinator>:<port> --once    # final result (HW2 format)
./run_cluster.sh stop                                         # stops the coordinator and the workers
exit                                                          # gives the nodes back
```
The port is `40000 + (your user id % 20000)`, so different users on the same node do not clash.
Set `BASE_PORT=...` to choose another one.

---

## 9. Benchmarks

### Datasets (reproducible)
All datasets come from HW2's generator `../q8/generate_dataset.py` with a fixed seed, so the same
parameters always give byte-identical files:
```bash
.venv/bin/python ../q8/generate_dataset.py --n 5000000 --k 10 --s 100 --seed 42 --out data_5M.txt
```
| Parameter | Value |
|---|---|
| seed | 42 |
| K (top stations), S (stations) | 10, 100 |
| N | 5,000,000 (workers, queries, rate), 200,000 (batch size), 100K / 1M (correctness) |
| station_id | uniform in [0, S-1] |
| timestamp | uniform in [0, 10 N], so about 6 records per 60-second interval (N/6 intervals) |
| temperature / humidity / pressure / rainfall / wind speed | uniform in [-20, 50] / [0, 100] / [950, 1050] / [0, 50] / [0, 40], 2 decimals |

The correctness tests also use all 13 HW2 test cases in `../q8/testcases/`.

### Why these configurations
* **5M records:** long enough that every run takes several seconds (3.5 to 35 s), so start-up costs and
  laptop noise matter less, but short enough to run all 22 configurations in about 15 minutes.
* **Batch size 1000** for all other experiments: the batch-size experiment shows that 1000 records per
  message already gets most of the maximum speed (1.16M vs 1.35M records/s at 10000).
* **1 to 8 workers:** the laptop has 10 cores; with 8 workers plus coordinator, client and dashboard the
  processes already outnumber the cores.
* **One dashboard every 100 ms** in the workers experiment: a realistic live dashboard. The queries
  experiment uses dashboards that query back to back (the worst case).
* **Rates 200K to 1.6M records/s:** from well below to just above the measured maximum (about 1.4M).

### Running the benchmarks

`bench.sh` runs every experiment, each with a fresh system, and checks every final result against
HW2's sequential program.

**The results in `results/` and in the report were measured on a laptop** (Apple M4, all processes on
localhost), because the cluster queue was full close to the deadline (the TAs allowed running gRPC locally).
Correctness (253/253) and the multi-node demo (`screenshots/`) were done on the RCE cluster.
`results/bench_results.csv` holds all 22 runs; the `queries` experiment was run twice because of laptop noise,
and the second run is used (`results/bench_queries_rerun.*`).

| Experiment | What changes | Fixed |
|---|---|---|
| `workers` | W = 1, 2, 4, 8, both strategies, 1 dashboard querying every 100 ms | N = 5M, batch 1000 |
| `batch` | records per message B = 1, 10, 100, 1000, 10000 | N = 200K, W = 4, interval |
| `queries` | Q = 0, 1, 4, 16 dashboards querying non-stop | N = 5M, W = 4, batch 1000 |
| `rate` | client rate 200K, 400K, 800K, 1.6M records/s, and max | N = 5M, W = 4, batch 1000 |

On the cluster (4 nodes: coordinator on node 1, workers on nodes 2 and 3, client and dashboards on node 4):
```bash
cd ~/ds-hw3-sec2/q2_grpc
sbatch bench.sh
tail -f bench_<jobid>.log                     # Ctrl+C stops watching, not the job
```
On one machine:
```bash
./bench.sh                                     # full size
N_MAIN=1000000 N_BATCH=20000 ./bench.sh        # quick test (about a minute)
EXPERIMENTS="workers batch" ./bench.sh         # only some experiments
```
Output: `results/bench_results.csv`, one line per run:
```
experiment,N,workers,strategy,batch,rate,query_clients,send_s,total_s,throughput,
queries,q_mean_ms,q_p50_ms,q_p95_ms,q_max_ms,records_per_worker,
coord_rss_mb,coord_cpu_s,worker_rss_mb_max,worker_cpu_s_total,correct
```
* `total_s`: time from the first record sent until **all** records are processed (end to end).
* `throughput`: N / total_s.
* `q_*_ms`: query latency seen by the dashboards (only measured while the stream is running).
* memory / CPU columns: from GNU `/usr/bin/time` (cluster only; empty on a Mac).
* `correct`: `EXACT` / `FP_CLOSE` / `FAIL` against HW2 sequential.

**Memory and CPU** of every process (peak RSS and CPU time, works on a Mac and on Linux):
```bash
./measure_resources.sh 5000000 4 interval        # -> results/resources_interval.txt
./measure_resources.sh 5000000 4 roundrobin      # -> results/resources_roundrobin.txt
```

Run **one benchmark job at a time**. Copy the results to the laptop for the plots and the report:
```bash
scp "<username>@rce.iiit.ac.in:~/ds-hw3-sec2/q2_grpc/results/bench_results.csv" results/
scp <username>@rce.iiit.ac.in:~/ds-hw3-sec2/q2_grpc/bench_<jobid>.log results/bench_full.log
```

---

## 10. Plots

```bash
.venv/bin/python plot_results.py
```
| File | Shows |
|---|---|
| `plots/throughput_vs_workers.png` | throughput vs number of workers, both strategies |
| `plots/query_latency_strategy.png` | query latency during the stream, both strategies |
| `plots/throughput_vs_batch.png` | throughput vs records per message |
| `plots/concurrent_queries.png` | ingestion throughput and query latency vs number of dashboards |
| `plots/rate.png` | achieved vs requested stream rate |
| `results/summary.md` | all numbers as markdown tables |

---

## 11. Program reference

```
python worker.py <listen_address>
python coordinator.py <listen_address> <interval|roundrobin> <worker_address> [<worker_address> ...]
python client.py <coordinator_address> <input_file> [batch_size=1000] [rate=0]    (rate in records/s, 0 = max)
python dashboard.py <coordinator_address>                  live view (--interval ms, --exit-when-done)
python dashboard.py <coordinator_address> --once           current analytics in the HW2 format
python dashboard.py <coordinator_address> --bench          query until the stream is done, print latencies
python dashboard.py <coordinator_address> --stats          one summary line
python dashboard.py <coordinator_address> --shutdown       stop the coordinator and the workers
python sequential.py <input_file>                          the analytics without gRPC
```
Listen addresses like `0.0.0.0:50061` accept connections from other machines; `127.0.0.1:50061` only
from the same machine. Clients connect to `host:port` (on the cluster the node name, e.g. `node06:50061`).

---

## 12. Cleanup

```bash
rm -rf logs bench_work_* data_*.txt .cluster_coordinator __pycache__
pkill -u $USER -f worker.py; pkill -u $USER -f coordinator.py     # if something is still running
rm -rf .venv                                                      # only to start again from scratch
```

---

## 13. Troubleshooting

| Problem | Fix |
|---|---|
| `run ./setup_env.sh first` | The virtual environment is missing: `./setup_env.sh --laptop` (on the cluster `PYTHON=/usr/bin/python3.11 ./setup_env.sh`). |
| `plot_results.py`: `No module named pandas` | Plots are made on the laptop (`./setup_env.sh --laptop`), not on the cluster. |
| Error about the protobuf version when importing `weather_pb2` | The Python's own protobuf is too old: `rm -rf .venv` and run `setup_env.sh` again (it then installs a new one into `.venv`). |
| `No module named grpc` | Use `.venv/bin/python` (or `source .venv/bin/activate`), not the system `python3`. |
| `setup_env.sh`: python is too old | Cluster: `PYTHON=/usr/bin/python3.11 ./setup_env.sh`, or `module load python/3.12.5` first. |
| `could not connect to ...` | Start the workers before (or with) the coordinator; on the cluster use the node name, not `localhost`. |
| Everything hangs / no output on the cluster | The cluster sets `http_proxy` (for pip), and python gRPC would route its connections through it. Our channels switch this off (`grpc.enable_http_proxy = 0` in `grpc_common.py`); if you write your own gRPC test, do the same or `unset http_proxy https_proxy`. |
| `could not listen on ... (is the port already used...)` | Another program uses the port: `BASE_PORT=45000 ./verify_correctness.sh` (any free number). |
| Port already in use | Another run is still active (`pkill -u $USER -f worker.py`), or another user uses the port: `BASE_PORT=45000 ./...`. |
| `srun: unrecognized option '--overlap'` | Old Slurm: remove `--overlap` from the `srun` lines in `run_cluster.sh` / `bench.sh`. |
| A background `ssh ... &` shows `Stopped` | Use `ssh -n` (the shell pauses background jobs that read from the keyboard). |
| `FP_CLOSE` instead of `EXACT` | Correct result: floating point rounding (Section 6). |
| Dashboard shows `WAITING FOR DATA` | The client has not started yet, or is still reading its file. |
