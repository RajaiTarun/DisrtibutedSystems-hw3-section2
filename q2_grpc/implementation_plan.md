# Q2: Real-Time Streaming Analytics with gRPC: Implementation Plan

This is the step-by-step plan for HW3 Section 2, Q2: the HW2 Q8 weather analytics, but on a
**live stream of records** instead of a complete file. After the last phase, Section 2 (Q1 + Q2)
is ready to submit.

Sources used: `hw3-final.pdf` (Section 2, Q2 and the gRPC documentation links in Section 3),
`rce_grpc_execution_guide.pdf`, `additional_info.txt`, `Home_Work_2 (1).pdf` (Q8).

---

## 1. What Q2 asks for

- The **same Q8 analytics** (same input format, output format and tie-break rules as HW2).
- A **streaming client** replays a pre-generated dataset **as if it were live**. There must be a
  configurable **rate or batch size** for sending records.
- A **gRPC server / coordinator** receives the stream.
- **Multiple workers** do the processing.
- A mechanism to **keep and combine the analytics state** of the workers.
- **Queries must work while data is still arriving**, and concurrent updates + queries must be
  handled correctly.
- A **CLI dashboard** shows the current analytics while the stream is running.
- The **final result must match a correct sequential implementation** (our HW2 `sequential`).
- A `.proto` with at least **streaming ingestion** and **queries for the current analytics**.
- Performance study of at least: **number of workers**, **record distribution strategy**,
  **message batch size**, **query frequency / number of concurrent query clients**.
  Useful metrics: throughput, query latency, end-to-end time, effect of workers and batch size,
  effect of concurrent queries, CPU and memory.
- Reproducible datasets (generator, seed, sizes documented).
- **Submission:** complete gRPC system with multiple workers, `.proto`, streaming client and CLI
  dashboard, dataset generator, README (setup, compile, run, architecture, experiments),
  correctness verification, benchmark results, plots/tables and observations.
- **Language:** C++ (additional_info allows C++ or Python). We use C++ so we can reuse the HW2
  analytics code.
- **Cluster:** follow the RCE guide: server on one compute node, clients on other compute nodes,
  connecting with `<node>:<port>` (not `localhost`), and at least 2 clients running at the same time.

---

## 2. Design

```
 client ──stream of batches──►  COORDINATOR  ──batches──► worker 0 ┐
 (replays the file,            (gRPC server)  ──batches──► worker 1 ├─ each keeps its own Stats
  --batch, --rate)                   ▲        ──batches──► worker W ┘   (protected by a mutex)
                                     │ GetAnalytics()
 dashboard(s) ───────────────────────┘  the coordinator asks every worker for its partial Stats,
 (live view / --once / --bench)          merges them with the HW2 merge code, and returns the result
```

### 2.1 Components
| Program | Role |
|---|---|
| `client` | reads the dataset, sends the header (K, S) and then the records in batches over one gRPC client stream |
| `coordinator` | gRPC server; receives the stream, splits each batch between the workers, answers analytics queries by merging the workers' partial results |
| `worker` (W of them) | gRPC server; keeps a HW2 `Stats` object for its share of the records; returns its partial stats when asked |
| `dashboard` | gRPC client; shows the live analytics, or prints the final output once, or measures query latency |

### 2.2 Which worker gets which record (distribution strategy)
- **Default: by interval**, `worker = (timestamp / 60) % W`.
  - Every interval lives on exactly **one** worker, so each worker only has to report *its own
    busiest interval*, and the coordinator picks the best of those. The answer is still exact.
  - Stations are few (S = 100), so sending all station counts from every worker is cheap.
  - So every query is **small and fast, no matter how much data has arrived**.
- **For comparison: round-robin** (records spread evenly in turn).
  - Perfect load balance, but an interval is spread over many workers, so each query must send
    every worker's **whole interval map**, and queries get slower as more data arrives.
- Comparing the two is the "record distribution strategy" experiment.

### 2.3 Reuse from HW2
`../q8/q8_common.cpp/.hpp`: `Stats`, `updateStats()`, `mergingWorkerProcessStats()`,
`getTopStations()`, `getBusiestInterval()`, `printResults()`. Same analytics and the same 6-decimal
output format, with no new analytics code.

### 2.4 Concurrency
- Worker: one mutex around its `Stats`; `ProcessBatch` and `GetPartial` both lock it.
- Coordinator: **one queue + one sender thread per worker**, so batches go to all workers in
  parallel; counters are protected by a mutex.
- The end of the client stream is only acknowledged after all worker queues are empty, so a query
  after the client finishes always sees all the data.

---

## 3. Step-by-step plan

Each step is small and has a clear "done when" check.

### Phase 0: Setup
| Step | What | Done when |
|---|---|---|
| 0.1 | Mac: `brew install grpc` (cmake, protobuf and abseil are already installed) | `pkg-config --modversion grpc++` prints a version |
| 0.2 | Tiny "hello" gRPC program: `.proto` → generated C++ → compile → client calls server | hello works on the Mac |
| 0.3 | **Cluster: check for gRPC C++** (`module avail`, `pkg-config grpc++`, `which grpc_cpp_plugin`). If missing: install it in the home folder with conda/micromamba (`grpc-cpp` from conda-forge); if that fails, build gRPC from source | the same hello program works on the cluster |
| 0.4 | `q2_grpc/Makefile` that generates the gRPC code and builds all programs | `make` builds hello |

Step 0.3 is the biggest risk, so it is done first.

Cluster check commands:
```bash
module avail 2>&1 | grep -iE "grpc|protobuf|conda|anaconda|miniconda|cmake|gcc"
pkg-config --modversion grpc++ protobuf 2>&1
which protoc grpc_cpp_plugin conda cmake
g++ --version | head -1
```

### Phase 1: Interface (`weather.proto`)
| Step | What |
|---|---|
| 1.1 | Messages `Record`, `RecordBatch` (repeated records), `StreamConfig` (K, S, number of workers, strategy), `Ack` |
| 1.2 | `PartialStats` (G fields, station arrays, best interval, or the full interval list for round-robin), `AnalyticsRequest`, `AnalyticsReply` (formatted result + live counters) |
| 1.3 | `service Coordinator { Configure; StreamRecords(stream RecordBatch) returns Ack; GetAnalytics }` |
| 1.4 | `service Worker { Configure; ProcessBatch(RecordBatch) returns Ack; GetPartial }` |
| 1.5 | Makefile rule: `protoc` generates `weather.pb.*` and `weather.grpc.pb.*` |

### Phase 2: Worker (`worker.cpp`)
| Step | What | Done when |
|---|---|---|
| 2.1 | gRPC server on the address given as an argument; `Stats` + mutex | starts and listens |
| 2.2 | `ProcessBatch`: lock, then `updateStats()` for every record | counts go up |
| 2.3 | `GetPartial`: lock, then convert `Stats` to `PartialStats` | a test query returns sensible numbers |
| 2.4 | `convert.cpp/.hpp`: `statsToProto()` / `protoToStats()` | round trip gives the same values |

### Phase 3: Coordinator (`coordinator.cpp`)
| Step | What | Done when |
|---|---|---|
| 3.1 | Connects to W workers (addresses as arguments); forwards `Configure` | all workers configured |
| 3.2 | `StreamRecords`: read batches from the client stream, split each batch per worker (interval or round-robin), put the pieces in one queue per worker; one sender thread per worker sends them | records reach all workers |
| 3.3 | At the end of the stream, wait until all queues are empty, then return `Ack` | after `Ack` all data is processed |
| 3.4 | `GetAnalytics`: `GetPartial` from all workers, merge with the HW2 merge code, format with `printResults()`, add live counters (records received, records/s, time since start) | correct answers during ingestion |

### Phase 4: Streaming client (`client.cpp`)
| Step | What |
|---|---|
| 4.1 | Read the dataset file; send the header as `Configure` |
| 4.2 | Send `RecordBatch`es over one client stream. Options: `--batch B` (records per message), `--rate R` (records/s, 0 = as fast as possible) |
| 4.3 | At the end print records sent, total time and throughput (records/s) |

### Phase 5: CLI dashboard (`dashboard.cpp`)
| Step | What |
|---|---|
| 5.1 | **Live mode**: every `--interval` ms call `GetAnalytics`, clear the screen and show: records received, ingest rate, main statistics, hottest/coldest, busiest interval, top-K table, and this query's latency |
| 5.2 | **`--once`**: print exactly the HW2 output format (for correctness checks) |
| 5.3 | **`--bench Q`**: send Q queries back to back and print latency p50 / p95 / max (for the concurrent query experiments) |

### Phase 6: Local run + correctness
| Step | What | Done when |
|---|---|---|
| 6.1 | `run_local.sh`: start W workers + coordinator + dashboard + client on localhost ports, stop everything at the end | the live dashboard updates while data streams in |
| 6.2 | `verify_correctness.sh`: all HW2 test cases + generated 100K / 1M data, W = 1, 2, 4, both strategies, several batch sizes; compare `dashboard --once` with `sequential` using `../q1_mapreduce/compare_outputs.py` | all pass (`EXACT` or `FP_CLOSE`) |
| 6.3 | Queries during ingestion: several dashboards while streaming | no crash, no wrong counts, count in every snapshot ≤ N |

### Phase 7: Cluster run (like the RCE guide)
| Step | What |
|---|---|
| 7.1 | `salloc --nodes=4 --ntasks-per-node=1`; coordinator on node01, workers on the allocated nodes, client on node02, **2 dashboards on node03 and node04** |
| 7.2 | Capture the live dashboard (screenshot / text) for the report |
| 7.3 | The final `dashboard --once` output matches `sequential` on the cluster |

### Phase 8: Benchmarks (`bench_slurm.sh`, one Slurm job over several nodes)
| # | Experiment | Values | Measured |
|---|---|---|---|
| 8.1 | Number of workers | W = 1, 2, 4, 8 (fixed batch) | throughput, end-to-end time |
| 8.2 | Batch size | B = 1, 10, 100, 1000, 10000 | throughput (cost per message) |
| 8.3 | Distribution strategy | by interval vs round-robin | throughput, query latency (and how it grows with data), records per worker |
| 8.4 | Concurrent queries | 0, 1, 4, 16 query clients | query latency p50/p95, ingestion slowdown |
| 8.5 | Stream rate | fixed `--rate` values vs max | does the system keep up with the rate |
| 8.6 | CPU / memory | `/usr/bin/time -f "%e %M %U %S"` for workers + coordinator | CPU time, peak memory |

- Datasets: HW2 generator, seed 42, K = 10, S = 100; 1M records for most runs, 10M for the
  largest ones.
- Every run's final output is checked with `compare_outputs.py`.
- Results go to `results/*.csv` (one line per run), logs to `results/*.log`.

### Phase 9: Plots + analysis
| Step | What |
|---|---|
| 9.1 | `plot_results.py`: throughput vs W, throughput vs batch size, query latency vs concurrent queries, strategy comparison, memory; plus `results/summary.md` tables |
| 9.2 | Explain the numbers: cost per RPC, coordinator as a bottleneck, lock contention, message sizes, load balance |

### Phase 10: Documentation + report
| Step | What |
|---|---|
| 10.1 | `q2_grpc/README.md`: setup (gRPC on Mac and cluster), build, local run, cluster run (RCE guide style), architecture, experiments, troubleshooting |
| 10.2 | `q2_grpc/tutorial.md`: gRPC basics for beginners (like Q1's tutorial) |
| 10.3 | Report section 2 (Q2): architecture, `.proto` design and why, distribution strategy, concurrency, correctness, experiments + plots + reasoning, short comparison of gRPC streaming vs MapReduce vs MPI. Same simple, short style as Q1 |

### Phase 11: Final submission check (all of Section 2)
- [ ] Q1: code, README, results, plots (done)
- [ ] Q2: code, `.proto`, client, dashboard, README, correctness, benchmarks, plots
- [ ] Report: team / roll number placeholders filled, Q1 + Q2 sections, PDF compiled
- [ ] Every README command tested from a fresh `git clone` (Mac and cluster)
- [ ] `.gitignore`: no binaries, datasets or temp files; results and plots committed
- [ ] Final push, and check how the course wants the submission (repo link or zip)

---

## 4. Files at the end

```
q2_grpc/
├── implementation_plan.md     this file
├── weather.proto              gRPC interface
├── Makefile                   generates the gRPC code and builds everything
├── convert.cpp / convert.hpp  Stats <-> PartialStats (protobuf) conversion
├── worker.cpp                 worker server
├── coordinator.cpp            coordinator server
├── client.cpp                 streaming client (replays the dataset)
├── dashboard.cpp              CLI dashboard / --once / --bench
├── run_local.sh               everything on one machine
├── verify_correctness.sh      correctness tests vs sequential
├── run_cluster.sh             helper for the cluster demo (RCE guide)
├── bench_slurm.sh             benchmarks
├── plot_results.py            plots + summary tables
├── results/  plots/
├── README.md
└── tutorial.md
```

Uses from the rest of the repo: `../q8/q8_common.*` (analytics), `../q8/sequential.cpp`
(reference), `../q8/generate_dataset.py` (datasets), `../q8/testcases/` (test inputs),
`../q1_mapreduce/compare_outputs.py` (output comparison).

---

## 5. Progress

| Phase | Status |
|---|---|
| 0 Setup | Mac done (gRPC 1.84, protobuf 36.2, hello test passes); cluster check pending |
| 1 Interface | not started |
| 2 Worker | not started |
| 3 Coordinator | not started |
| 4 Client | not started |
| 5 Dashboard | not started |
| 6 Local run + correctness | not started |
| 7 Cluster run | not started |
| 8 Benchmarks | not started |
| 9 Plots + analysis | not started |
| 10 Docs + report | not started |
| 11 Submission check | not started |
