# Tutorial: Section 2, Q2 (Q8 Weather Analytics as a live stream, gRPC in Python)

This file explains, from the basics:
1. What Q2 asks for, and how it differs from Q1
2. What gRPC and Protocol Buffers are (in Python)
3. Our design, part by part
4. The code of each program
5. Concurrency: threads, processes, locks
6. How we test and benchmark it

---

## 1. What Q2 asks for

In Q1 (MapReduce) the **whole file exists before we start**, and we compute the answer once.
In Q2 the data is a **stream**: records arrive one after another, as if they came from live weather
stations, and the analytics must be **up to date at any moment**, while data is still arriving.

| | Q1 MapReduce | Q2 gRPC streaming |
|---|---|---|
| Data | complete file | records keep arriving |
| Answer | once, at the end | any time (live dashboard), final answer at the end |
| Work split | the input file is cut into pieces | our coordinator splits the stream |
| Communication | text lines + sort | gRPC calls over the network |

The final answer must still be exactly the HW2 output.

---

## 2. gRPC and Protocol Buffers from the basics

### 2.1 RPC = calling a function in another program
A **Remote Procedure Call** looks like a normal function call:
```python
reply = stub.GetAnalytics(weather_pb2.AnalyticsRequest())   # runs on the coordinator, maybe on another node
```
but the function runs in **another process**, possibly on another machine. The RPC library turns the
arguments into bytes, sends them over the network, runs the function there, and sends the result back.

### 2.2 gRPC
**gRPC** is Google's RPC framework (`pip install grpcio`). We describe our functions and messages once
in a `.proto` file, and a tool generates the Python code for both sides:
* **server side:** a class `WorkerServicer` with one method per RPC, which *we* fill in:
  ```python
  class WorkerServicer(weather_pb2_grpc.WorkerServicer):
      def ProcessBatch(self, request, context):
          ...
          return weather_pb2.Ack(records=len(request.records))
  ```
* **client side:** a **stub** whose methods send the call to the server:
  ```python
  stub = weather_pb2_grpc.WorkerStub(channel)
  stub.ProcessBatch(batch)
  ```
A **channel** is the connection to a server: `grpc.insecure_channel("node06:50061")`.

### 2.3 Protocol Buffers (protobuf)
The messages are defined in the same `.proto` file:
```proto
message Record {
  int64 timestamp = 1;
  int32 station_id = 2;
  double temperature = 3;
  ...
}
message RecordBatch {
  repeated Record records = 1;   // repeated = a list
}
```
The generated Python classes work like simple objects: `r = weather_pb2.Record(timestamp=10, station_id=3)`,
`r.temperature`, `batch.records.add(...)`, `len(batch.records)`, `for r in batch.records`.
Messages are sent in a compact **binary** format (not text). The numbers `= 1`, `= 2` are field ids
used in that format.

### 2.4 Generating the code
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. weather.proto
```
creates `weather_pb2.py` (messages) and `weather_pb2_grpc.py` (servicer base classes + stubs).
`gen_proto.sh` runs this command, and `setup_env.sh` runs `gen_proto.sh`.

### 2.5 Four kinds of RPCs
| Kind | Example in our code | Meaning |
|---|---|---|
| unary | `GetAnalytics(AnalyticsRequest) returns (AnalyticsReply)` | one request, one reply |
| **client streaming** | `StreamRecords(stream RecordBatch) returns (Ack)` | the client sends **many** messages over one call, the server answers once at the end |
| server streaming | (not used) | one request, many replies |
| bidirectional | (not used) | both sides send many messages |

In Python, the client passes a **generator** (a function with `yield`) to a client-streaming call, and
gRPC sends every message the generator yields. On the server, the method gets an **iterator**
(`for batch in request_iterator:`).

---

## 3. Our design

```
 client.py ──stream of batches──► coordinator.py ──batches──► worker.py 0 ┐
                                       ▲          ──batches──► worker.py 1 ├─ each keeps its own Stats
                                       │          ──batches──► worker.py W ┘
 dashboard.py ── GetAnalytics ─────────┘  asks every worker for its partial Stats and merges them
```

### 3.1 The idea we reuse from HW2 and Q1: partial results that can be merged
`weather_stats.py` is the HW2 analytics written in Python. A `Stats` object keeps counts, sums, mins,
maxes, the hottest/coldest measurement, per-station totals and per-interval counts. `update()` adds one
record; `merge()` adds another `Stats` (exactly like HW2's `mergingWorkerProcessStats()`).

Each worker keeps a `Stats` for **its** records. To answer a query, the coordinator gets every worker's
`Stats` and merges them, then `format_results()` prints the HW2 output. Because merging works at **any**
time, a query in the middle of the stream simply shows the analytics of the records processed so far.

Before writing any gRPC code, we checked the port with `sequential.py` against HW2's C++ `sequential`:
identical output on every test case.

### 3.2 Which worker gets which record: the distribution strategy
* **By interval** (default): `worker = (timestamp // 60) % W`. All records of one 60-second interval go
  to the same worker, so each worker knows the **complete** count of each of its intervals and its busiest
  interval is final. The coordinator only needs each worker's single busiest interval. A query message is
  therefore always small: the global fields + S station totals + 1 interval.
* **Round-robin**: record 1 to worker 1, record 2 to worker 2, ... Perfectly even load, but the records of one
  interval end up on different workers, so every worker must send **all** its interval counts per query.

Top-K works with both: there are only S = 100 stations, so every worker sends all station totals and the
coordinator adds them up, and only then picks the top K (the HW2 lesson: never pick top-K before merging).

---

## 4. The code

### 4.1 `worker.py`
```python
def ProcessBatch(self, request, context):
    with self.lock:
        for r in request.records:
            self.stats.update((r.timestamp, r.station_id, r.temperature, ...))
    return weather_pb2.Ack(records=len(request.records))
```
`Configure` makes a fresh `Stats(S)`, `GetPartial` returns `self.stats.to_proto(...)`.
`main()` creates a server with a thread pool (`grpc.server(futures.ThreadPoolExecutor(...))`), adds our
servicer, listens on the given address and waits until `Shutdown` is called.

### 4.2 `coordinator.py`
* `StreamRecords(self, request_iterator, context)` reads batches (`for batch in request_iterator`), splits
  each batch into one list per worker, and puts a `RecordBatch` into that worker's **queue** (`queue.Queue`).
* Every worker has a **sender thread** that takes batches from its queue and calls `ProcessBatch` on the
  worker. So all workers get data at the same time, and the coordinator never waits for one worker before
  sending to the next.
* The queues have a **maximum size** (64). If a worker is slower than the client, `queue.put()` waits, and so
  does the stream (**back pressure**), instead of filling the memory.
* When the client has sent everything, `StreamRecords` waits until all queues are empty (`queue.join()`), and
  only then answers. So when the client's call returns, **all** records are processed.
* `GetAnalytics` asks all workers **at the same time** (`stub.GetPartial.future(...)`), merges the results
  and formats them.

### 4.3 `client.py`
1. Reads the file and builds all `RecordBatch` messages first (so the measured time is the system's, not the disk's).
2. `Configure` with the header.
3. `StreamRecords(generate())`: the generator yields the batches one by one; with `rate > 0` it sleeps so
   that record `i` leaves at time `i / rate`.
4. The call returns when the coordinator has processed everything; the client prints the throughput.

### 4.4 `dashboard.py`
Calls `GetAnalytics` in a loop and redraws the screen (`\033[2J\033[H` clears the terminal).
`--once` prints only the result (used for the correctness checks), `--bench` measures query latency.

---

## 5. Concurrency

* **Threads inside one process.** gRPC runs every incoming call in a thread of its thread pool. So on a
  worker, `ProcessBatch` (new data) and `GetPartial` (a query) can arrive at the same time. The worker's
  **lock** (`threading.Lock`) makes them take turns: a query always sees whole batches, never half of one.
* **The GIL.** In one Python process only one thread runs Python code at a time. Threads are still fine for
  *waiting* (network calls), but not for computing in parallel. That is why every worker is a separate
  **process**: W processes really compute at the same time, on different cores or nodes.
* **Queues.** `queue.Queue` is already thread-safe: `put()` waits when the queue is full, `get()` waits when it
  is empty, `join()` waits until every item is processed.
* Every worker's record count only grows, so the total count seen by a dashboard never goes down between two
  queries. `verify_correctness.sh` checks this with 4 dashboards querying non-stop.

---

## 6. Testing and benchmarking

* `verify_correctness.sh`: 253 tests; every final result is compared with HW2's C++ `sequential`
  (`EXACT` or `FP_CLOSE`; see the report for the floating point rounding cases).
* `bench.sh`: throughput vs number of workers and strategy, vs batch size, vs number of dashboards querying,
  and vs requested stream rate. Memory and CPU time from `/usr/bin/time` on the cluster.

### Quick glossary
| Term | Meaning |
|---|---|
| RPC | calling a function that runs in another process |
| servicer | the server-side class whose methods implement the RPCs |
| stub | client-side object whose methods send RPCs |
| channel | the connection to a server (`host:port`) |
| protobuf | binary message format generated from the `.proto` file |
| client streaming | one RPC in which the client sends many messages |
| generator | a Python function with `yield`; gives values one by one |
| back pressure | slowing down the sender when the receiver cannot keep up |
| GIL | Python's global lock: one thread runs Python code at a time per process |
| throughput | records processed per second |
| latency | time one query takes |
