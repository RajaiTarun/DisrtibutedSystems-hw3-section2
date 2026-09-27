# Q8 — Benchmark Results

We tested both programs on 7 input sizes (100 to 50,000,000 records) and
4 process counts. `P` is the total number of MPI processes — one of them
(rank 0) is the "master" and never does any counting itself, so `P=9`
really means **8 workers** actually doing work. Both programs print how
long each step took (reading the input, doing the counting) so we could
compare them fairly.

## Our approach

The master process (rank 0) reads the whole input file and does no
counting itself. It splits the records into equal-ish chunks, one per
worker, and sends each worker its chunk in a single bulk message. Each
worker counts its own chunk on its own (temperature/humidity/pressure
totals, per-station counts, per-time-window counts, hottest/coldest,
extreme-event count). Workers send their partial results back to the
master, which combines everything into one final answer and prints it.
The sequential version does the exact same steps, just by itself in a
loop, so the two are easy to compare fairly.

**Optimizations we made and kept:**

- Turned off the default syncing between C++'s `cin`/`cout` and the
  older C-style I/O functions, and stopped `cin` from auto-flushing
  `cout` — this made reading millions of lines noticeably faster.
- Instead of sending each record to a worker one at a time (which would
  mean millions of tiny messages), we send a worker's whole chunk of
  records in **one** message, using a custom MPI data type built to
  match our record's layout.
- A worker's final results are packed into a few plain arrays of
  numbers and sent back in a handful of messages — simple, and fast
  enough since each worker only sends its results once.

**An optimization we considered but didn't build:** right now only the
master reads the input file, and this turns out to be the main reason
MPI struggles to beat plain sequential code (see below). The natural
fix is to have each worker read its own share of the file directly
instead of waiting for the master — the assignment rules explicitly
allow this. We didn't get this implemented and tested in time for this
submission, so we're leaving it here as the clear next step rather than
a finished feature.

## Complexity analysis

Let N = number of records, S = number of stations, K = stations
reported at the end, and I = number of distinct 60-second time windows
in the data. Every record only needs O(1) work to process (update a few
running totals, check hottest/coldest, bump one station's and one
window's counter).

- **Sequential:** O(N) to read, O(N) to count, O(S log K) to pick the
  top K stations (using a small heap), O(I) to find the busiest window.
  Total: **O(N + S log K + I)** time, O(N + S + I) memory.
- **MPI:** the master's read is still O(N) — one process, doesn't get
  faster with more workers. Handing out the data is O(N) total but
  happens as parallel transfers. Each worker then counts its own share:
  O(N / workers) — this part *does* shrink as workers increase.
  Combining everyone's results back at the master is O(workers·S + I),
  then O(S log K) for the final top-K list. Total:
  **O(N) + O(N / workers) + O(workers·S + I + S log K)**.

The O(N) reading term never shrinks no matter how many workers you add
— it's the same cost every time. That's exactly why, in the results
below, the counting step scales beautifully with more workers while the
total time barely improves (or gets worse): the part of the program
that *doesn't* parallelize is the one that dominates.

## 1. Total time: sequential vs MPI

| N (records) | sequential | MPI, 1 worker | 2 workers | 4 workers | 8 workers |
|---|---:|---:|---:|---:|---:|
| 100 | 0.002s | 0.10s | 0.10s | 0.09s | 0.19s |
| 1,000 | 0.003s | 0.10s | 0.09s | 0.09s | 0.18s |
| 10,000 | 0.01s | 0.09s | 0.10s | 0.10s | 0.20s |
| 100,000 | 0.10s | 0.18s | 0.18s | 0.25s | 0.38s |
| 1,000,000 | 1.00s | 1.10s | 1.13s | 1.12s | 2.21s |
| 20,000,000 | 23.8s | 22.3s | 21.9s | 22.5s | 43.3s |
| 50,000,000 | 63.7s | 56.7s | 55.7s | 57.7s | 112.8s |

**What this shows:** for small inputs, MPI is way slower than sequential
— just starting up 9 processes takes about 0.1–0.2 seconds, and
sequential finishes before that startup is even done. MPI only starts
winning at the two biggest sizes (20M and 50M records), and even then
**only with 1, 2, or 4 workers**. Using all 8 workers is the *slowest*
option at every single size we tested — worse than sequential, worse
than using fewer workers.

## 2. Why does using more workers make it slower?

Only the master process reads the input file — the other 8 workers just
sit and wait until the master hands them their share of the data. So no
matter how many workers you add, the reading step doesn't get any
faster, since it's still one process reading the same file the same way.

Worse, at 8 workers this reading step doesn't just stay the same — it
gets **more than twice as slow**, and only once the input gets to about
1 million records or bigger. We checked this by running the benchmark 3
separate times, and it happened every single time, so it's a real
pattern and not a one-off fluke.

Our best explanation: while the master is busy reading the file, the 8
waiting workers don't fully go to sleep — they keep actively checking
whether data has arrived yet. All 9 processes run on the same physical
machine, sharing the same CPU cores and memory. With 8 processes all
doing this "active checking" at once, they compete with the master for
memory and CPU, which slows down the master's reading — and this
slowdown only becomes noticeable once the read takes long enough (i.e.
once N is large) for that competition to add up.

## 3. Compute time only (ignoring the reading step)

We also separately timed just the number-crunching part (counting,
averages, top stations, etc.), leaving out the reading time, for both
programs.

| N (records) | sequential | MPI, 1 worker | 2 workers | 4 workers | 8 workers |
|---|---:|---:|---:|---:|---:|
| 100,000 | 0.008s | 0.003s | 0.002s | 0.001s | 0.001s |
| 1,000,000 | 0.109s | 0.044s | 0.028s | 0.019s | 0.018s |
| 20,000,000 | 5.77s | 2.47s | 1.35s | 0.94s | 0.72s |
| 50,000,000 | 18.71s | 6.92s | 4.18s | 2.48s | 2.16s |

**What this shows:** once reading is taken out of the picture, MPI
scales really well — at 20M and 50M records, using 8 workers is almost
exactly 8x faster than doing the counting alone. This proves the actual
counting/analytics logic is not the problem at all — the entire
performance issue is in how the input file gets read and shared, not in
how the numbers get crunched.

To put it another way: at 50M records with 8 workers, the counting step
only takes 2.16 seconds out of a 112.8 second total run — **under 2% of
the total time**. Everything else is spent reading and waiting.

## 4. One odd result: 1,000,000 records behaves a bit differently

At exactly N=1,000,000, using 8 workers for the counting step didn't
speed things up quite as much as it did for the bigger sizes (about 6x
faster instead of close to 8x). We saw this in all 3 test runs, so it's
a real pattern, not random noise.

Our best guess is that this is a "too many cooks" problem. A computer
has a small amount of super-fast memory built into the CPU (called
cache) that it uses to avoid constantly fetching data from slower main
memory. Each worker's share of data at 1,000,000 records is small
(~7MB) — small enough that it could fit nicely in this fast memory on
its own. But with 8 workers running at the same time, they're all
trying to use that same small, shared fast-memory space together, and
it's not big enough for all of them at once — so they slow each other
down a little.

This doesn't happen at smaller sizes (everyone's data is tiny, no
crowding) or at bigger sizes (nobody's data fits in fast memory anyway,
so there's nothing to fight over) — it only shows up in this
in-between case. We haven't proven this with deeper tools, so take it
as our best explanation rather than a confirmed fact.

## Plots

- `plots/runtime.png` — total time vs. input size, sequential vs. MPI.
- `plots/speedup.png` / `efficiency.png` — total-time speedup and
  efficiency vs. number of workers.
- `plots/read_scaling_by_P.png` — shows the 8-worker reading slowdown
  only kicking in at N ≥ 1,000,000.
- `plots/breakdown_50M.png` — how the 50M-record run's time splits
  between reading, counting, and merging results, per worker count.
- `plots/compute_only.png`, `compute_only_speedup.png`,
  `compute_only_efficiency.png` — the counting-only comparison from
  section 3.

## Correctness

Both programs give identical, verified-correct output — see
`README.md`'s "Correctness verification" section for details.

## Conclusion

The MPI program is correct, and its actual counting logic scales
close to perfectly (~8x faster on 8 workers, once reading time is
excluded). But the overall program is usually *not* faster than the
simple sequential version, because only one process reads the input
file and everyone else just waits — and that waiting gets worse, not
better, as more workers are added. MPI only pulls ahead on the two
largest inputs we tested, and even then only with 4 or fewer workers.
The fix would be having each worker read its own share of the file
directly instead of making one process read everything and hand it
out — something the assignment explicitly allows but we didn't
implement.
