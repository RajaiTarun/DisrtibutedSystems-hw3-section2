# Q2: Real-Time Streaming Analytics with gRPC (Python): Implementation Plan

HW3 Section 2, Q2: the HW2 Q8 weather analytics on a **live stream of records**, implemented in
**Python with the `grpcio` library** (TA instruction: Q2 must be Python + grpcio; old Python versions
do not work with grpcio, so a recent Python is needed). Q1 (MapReduce, C++) is finished and unchanged.

After the last phase, the whole of Section 2 (Q1 + Q2) is ready to submit.

Sources: `hw3-final.pdf` (Section 2 Q2, gRPC documentation links in Section 3),
`rce_grpc_execution_guide.pdf`, `additional_info.txt`, `Home_Work_2 (1).pdf` (Q8), TA messages.

---

## 1. What Q2 asks for

- The **same Q8 analytics** as HW2 (same input, output format, tie-break rules).
- A **streaming client** replays a pre-generated dataset **as if it were live**, with a configurable
  **rate or batch size**.
- A **gRPC server / coordinator** receives the stream; **multiple workers** process it.
- A mechanism to **keep and combine the analytics state** of the workers.
- **Queries must work while data is still arriving**; concurrent updates and queries handled correctly.
- A **CLI dashboard** shows the current analytics during the stream.
- The **final result must match a correct sequential implementation** (our HW2 `sequential`).
- A `.proto` with at least **streaming ingestion** and **queries for the current analytics**.
- Performance study of at least: **number of workers**, **record distribution strategy**,
  **message batch size**, **query frequency / concurrent query clients**; useful metrics:
  throughput, query latency, end-to-end time, CPU and memory.
- Reproducible datasets (generator, seed, sizes documented).
- **Submission:** gRPC system with multiple workers, `.proto`, streaming client + CLI dashboard,
  dataset generator, README (setup, run, architecture, experiments), correctness verification,
  benchmark results, plots/tables and observations.
- **Cluster (RCE guide):** server on one compute node, clients on other compute nodes, connect with
  `<node>:<port>` (not `localhost`), at least 2 clients at the same time.
- **Language:** Python + `grpcio` (TA). TA also said earlier: use existing modules or run locally, and
  do not install system software.

---

## 2. Design

```
 client.py ──stream of batches──►  coordinator.py ──batches──► worker.py 0 ┐
 (replays the file,                 (gRPC server)  ──batches──► worker.py 1 ├─ each keeps a Stats object
  --batch, --rate)                        ▲        ──batches──► worker.py W ┘   (protected by a lock)
                                          │ GetAnalytics()
 dashboard.py (live / --once / --bench) ──┘  coordinator asks every worker for its partial Stats,
                                             merges them, returns the result in the HW2 format
```

| Program | Role |
|---|---|
| `client.py` | reads the dataset, sends the header (K, S), then the records in batches over one gRPC client stream, at a chosen rate |
| `coordinator.py` | gRPC server; splits every batch between the workers; one queue + one sender thread per worker; answers queries by merging the workers' partial results |
| `worker.py` (W processes) | gRPC server; keeps a `Stats` object for its records; returns it when asked |
| `dashboard.py` | gRPC client: live view, `--once` (HW2 format), `--bench` (query latency), `--stats`, `--shutdown` |
| `weather_stats.py` | the Q8 analytics in Python: `Stats`, `update`, `merge`, top-K, busiest interval, HW2 output format (a port of HW2's `q8_common.cpp`) |
| `sequential.py` | the analytics without gRPC (reads the file, prints the result); used to test `weather_stats.py` against HW2's C++ `sequential` |

### 2.1 Distribution strategy
- **By interval (default):** `worker = (timestamp // 60) % W`. Every interval lives on one worker, so a
  worker only reports its own busiest interval: queries stay small and fast.
- **Round-robin (for comparison):** records in turn; perfect balance, but each query must ship every
  worker's whole interval map.

### 2.2 Python-specific points
- **The GIL:** in one Python process only one thread runs Python code at a time. That is why every worker
  is its own **process**: W workers really compute in parallel. Inside a process, threads are still
  fine for waiting on the network (gRPC server threads, sender threads).
- **Speed:** Python is much slower per record than C++, so the workers' analytics is now real work and
  adding workers should help, until the single coordinator (which touches every record to route it)
  becomes the limit. The benchmarks will show where that happens.
- **Exact output:** Python floats are the same 64-bit doubles as C++, and `f"{x:.6f}"` rounds the same
  way as C++ `fixed << setprecision(6)`, so outputs can match HW2 exactly (or `FP_CLOSE` for huge sums).
- **Generated code:** `python -m grpc_tools.protoc` makes `weather_pb2.py` / `weather_pb2_grpc.py`
  (script `gen_proto.sh`); these files are not committed, they are generated on each machine.
- **Environment:** a virtual environment (`.venv`) with the packages from `requirements.txt`
  (`grpcio`, `grpcio-tools`, `protobuf`; `pandas`, `matplotlib` only for plots). This installs only
  Python packages inside our own folder, no system software.

### 2.3 Concurrency
- Worker: one `threading.Lock` around its `Stats`; `ProcessBatch` and `GetPartial` both take it.
- Coordinator: one bounded `queue.Queue` + one sender thread per worker (all workers get data in
  parallel; if a worker is slow the stream waits: back pressure).
- The end of the client stream is acknowledged only after all queues are drained, so a query after the
  client finishes sees all data.

---

## 3. Step-by-step plan

### Phase 0: Setup
| Step | What | Done when |
|---|---|---|
| 0.1 | Mac: virtual environment with Homebrew Python 3.13 (`python3.13 -m venv .venv`), `pip install -r requirements.txt` | `python -c "import grpc"` works in the venv |
| 0.2 | Cluster: find a recent Python (`module avail`, `python3 --version`, `python3.x`), check what the TAs meant by "old version" | a Python ≥ 3.9 is available |
| 0.3 | Cluster: venv with that Python + `pip install -r requirements.txt` (only Python packages, in our folder) | `import grpc` works on a compute node |
| 0.4 | Run the real system across nodes (the live demo in phase 8 does this; a separate hello test is not needed any more, gRPC already worked across nodes with C++) | client and dashboards on other nodes reach the coordinator |

### Phase 1: Interface
| Step | What |
|---|---|
| 1.1 | `weather.proto`: `Record`, `RecordBatch`, `StreamConfig`, `WorkerConfig`, `Ack`, `Empty`, `PartialStats`, `AnalyticsRequest`, `AnalyticsReply` |
| 1.2 | `service Coordinator { Configure; StreamRecords(stream RecordBatch); GetAnalytics; Shutdown }`, `service Worker { Configure; ProcessBatch; GetPartial; Shutdown }` |
| 1.3 | `gen_proto.sh`: generates `weather_pb2.py` + `weather_pb2_grpc.py` |

### Phase 2: Analytics in Python (no gRPC yet)
| Step | What | Done when |
|---|---|---|
| 2.1 | `weather_stats.py`: `Stats` class (`update`, `merge`, `busiest_interval`, `top_stations`, `format_results` in the exact HW2 format) | |
| 2.2 | `to_proto` / `from_proto` (Stats ↔ `PartialStats`) | round trip gives the same values |
| 2.3 | `sequential.py` + compare with HW2's C++ `sequential` on all 13 test cases and generated data | all `EXACT` |

### Phase 3: Worker (`worker.py`)
| Step | What |
|---|---|
| 3.1 | gRPC server on the given address; `Configure` creates a fresh `Stats(S)` |
| 3.2 | `ProcessBatch`: lock, `update` for each record; `GetPartial`: lock, `to_proto` (busiest interval only, or all intervals for round-robin) |
| 3.3 | `Shutdown` (for the scripts) |

### Phase 4: Coordinator (`coordinator.py`)
| Step | What |
|---|---|
| 4.1 | Connects to W workers (addresses as arguments), forwards `Configure` |
| 4.2 | `StreamRecords`: splits each batch per worker (interval / round-robin), puts it in that worker's bounded queue; one sender thread per worker calls `ProcessBatch` |
| 4.3 | After the stream: waits until all queues are empty, then returns `Ack` |
| 4.4 | `GetAnalytics`: `GetPartial` from every worker, merge, format; live counters (received, processed, elapsed, rate, per worker, done) |
| 4.5 | `Shutdown`: stops workers and itself |

### Phase 5: Streaming client (`client.py`)
| Step | What |
|---|---|
| 5.1 | Reads the dataset into memory; sends the header with `Configure` |
| 5.2 | Streams `RecordBatch`es (a generator feeding the client stream) with `--batch` and `--rate` |
| 5.3 | Waits for the final `Ack`; prints throughput + a machine-readable `RESULT ...` line |

### Phase 6: CLI dashboard (`dashboard.py`)
| Step | What |
|---|---|
| 6.1 | Live view: refreshes every `--interval` ms (status, records processed/received, rate, per-worker counts, query latency, the analytics) |
| 6.2 | `--once` (exact HW2 output), `--bench` (latency p50/p95/max, checks counts never go down), `--stats`, `--shutdown` |

### Phase 7: Local run + correctness
| Step | What | Done when |
|---|---|---|
| 7.1 | `run_local.sh`: starts W workers + coordinator, client in the background, live dashboard in the foreground, then compares with sequential | live dashboard works, final `EXACT` |
| 7.2 | `verify_correctness.sh`: all HW2 test cases × W = 1, 2, 4 × both strategies × batch sizes; generated 100K/1M; 4 dashboards querying during a stream | all pass |

### Phase 8: Cluster demo (RCE guide)
| Step | What |
|---|---|
| 8.1 | `run_cluster.sh start/stop` inside `salloc --nodes=4`: coordinator on node 1, workers on the others; prints the `ssh` commands for the client and 2 dashboards on other nodes |
| 8.2 | Run it; capture the live dashboard (screenshot) for the report; final `--once` matches sequential |

### Phase 9: Benchmarks (`bench.sh`, cluster via sbatch or one machine)
| # | Experiment | Values |
|---|---|---|
| 9.1 | Workers × strategy | W = 1, 2, 4, 8; interval and round-robin; 1 dashboard every 100 ms |
| 9.2 | Batch size | 1, 10, 100, 1000, 10000 records per message |
| 9.3 | Concurrent queries | 0, 1, 4, 16 dashboards querying non-stop |
| 9.4 | Stream rate | several fixed rates and maximum speed: does the system keep up |
| 9.5 | CPU / memory | `/usr/bin/time` for workers + coordinator (cluster) |

Dataset sizes are chosen after measuring Python's speed (probably 1M records for most runs). Every run's
final result is checked against HW2 sequential. Results: `results/bench_results.csv`.

### Phase 10: Plots + analysis
| Step | What |
|---|---|
| 10.1 | `plot_results.py`: throughput vs W (both strategies), query latency vs strategy, throughput vs batch size, concurrent queries, rate; `results/summary.md` |
| 10.2 | Explain the numbers: per-message cost, GIL and processes, coordinator bottleneck, query cost per strategy, locks |

### Phase 11: Documentation + report
| Step | What |
|---|---|
| 11.1 | `README.md`: setup (venv on Mac and cluster), run locally, cluster demo, correctness, benchmarks, plots, troubleshooting |
| 11.2 | `tutorial.md`: gRPC + protobuf basics in Python, our design, the code, concurrency |
| 11.3 | Report section 2 (Q2), same simple style as Q1: architecture, `.proto` design, strategy, concurrency, correctness, experiments + plots + reasoning, short comparison of streaming (gRPC) vs batch (MapReduce, MPI) |

### Phase 12: Final submission check (all of Section 2)
- [x] Q1: code, README, results, plots, report section
- [ ] Q2: code, `.proto`, client, dashboard, README, correctness, benchmarks, plots
- [ ] Report: team / roll number placeholders filled, Q1 + Q2 sections, PDF compiled
- [ ] Every README command tested from a fresh `git clone` (Mac and cluster)
- [ ] `.gitignore`: no venv, generated code, datasets or temp files; results and plots committed
- [ ] Final push; check how the course wants the submission (repo link / zip)

---

## 4. Files at the end

```
q2_grpc/
├── implementation_plan.md   this file
├── requirements.txt         grpcio, grpcio-tools, protobuf, pandas, matplotlib
├── setup_env.sh             creates .venv and installs requirements (Mac / cluster)
├── weather.proto            gRPC interface
├── gen_proto.sh             generates weather_pb2.py / weather_pb2_grpc.py
├── weather_stats.py         Q8 analytics (port of HW2 q8_common.cpp)
├── sequential.py            analytics without gRPC (tests weather_stats.py)
├── grpc_common.py           channel helper
├── worker.py  coordinator.py  client.py  dashboard.py
├── run_local.sh  verify_correctness.sh  run_cluster.sh  bench.sh
├── plot_results.py  results/  plots/
├── README.md
└── tutorial.md
```
Uses from the rest of the repo: `../q8/sequential.cpp` (reference), `../q8/generate_dataset.py`,
`../q8/testcases/`, `../q1_mapreduce/compare_outputs.py`.

---

## 5. Progress

| Phase | Status |
|---|---|
| 0 Setup | Mac done (Python 3.13 venv, grpcio 1.84); cluster: default python3 is 3.6 (too old), use /usr/bin/python3.11 or module python/3.12.5 — to be set up |
| 1 Interface | done (`weather.proto`, `gen_proto.sh`) |
| 2 Analytics in Python | done (`weather_stats.py`, `sequential.py`: identical to HW2 C++ on all test cases) |
| 3 Worker | done (`worker.py`) |
| 4 Coordinator | done (`coordinator.py`: per-worker queues + sender threads, back pressure, parallel queries) |
| 5 Client | done (`client.py`: batch size + rate) |
| 6 Dashboard | done (`dashboard.py`: live, --once, --bench, --stats, --shutdown) |
| 7 Local run + correctness | done on the Mac: `run_local.sh`, `verify_correctness.sh` 253/253 passed |
| 8 Cluster demo | script ready (`run_cluster.sh`); to run on RCE |
| 9 Benchmarks | script ready (`bench.sh`, tested locally with small N); full run on RCE pending |
| 10 Plots + analysis | `plot_results.py` ready; waits for the benchmark results |
| 11 Docs + report | README.md + tutorial.md done; report section after the benchmarks |
| 12 Submission check | not started |
