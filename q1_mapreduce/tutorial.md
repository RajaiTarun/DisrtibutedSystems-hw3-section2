# Tutorial: Section 2, Q1 (Q8 Weather Analytics using MapReduce)

This file explains, from the basics:

1. What Section 2 wants from us
2. What MapReduce is and the concepts behind it
3. What tools we use (Hadoop, Hadoop Streaming, Slurm) and why
4. Our design for Q8 and why it gives the correct answer
5. A full worked example on the sample input
6. The code of each program, line by line
7. How we test it and what we will benchmark

---

## 1. What is the aim of Section 2?

In HW2 (Section 3, Q8) we solved the **weather analytics** problem:

- Input: `N K S` on the first line, then N lines of
  `timestamp station_id temperature humidity pressure rainfall wind_speed`
- Output: total count, averages, mins and maxes, rainfall, wind, extreme events,
  hottest and coldest measurement, busiest 60 second interval, and the top K stations.

In HW2 we solved it in two ways:

| HW2 program | How it works |
|---|---|
| `sequential.cpp` | one process reads everything and computes the answer |
| `mpi.cpp` | rank 0 reads the data, **we** split it among worker ranks (`MPI_Scatterv`), each worker computes partial stats, and **we** gather and merge them |

In HW3 Section 2 we solve **the same problem, with the same input and output**, using two
other distributed models:

| Question | Model | Main idea |
|---|---|---|
| **Q1** | **MapReduce** (batch) | the whole dataset exists before we start. The framework splits it, runs our small programs on the pieces, and groups the results for us |
| **Q2** | **gRPC** (streaming) | records arrive one by one, like from live sensors. Workers keep running stats, and a dashboard shows the current answer at any moment |

The main learning goal is to see **how the choice of model changes the design**. The analytics
stay the same, but who splits the data, who moves it, and how partial results are combined
are different in each model. In Q1 we must also **compare MapReduce with our HW2 MPI
program** (time, scaling, data movement, ease of programming, and so on).

This tutorial covers Q1. Q2 will get its own tutorial.

---

## 2. MapReduce from the basics

### 2.1 The problem MapReduce solves

Imagine a 50 GB log file and 100 machines. You want counts, sums and similar numbers.
You *could* write MPI code: split the file, send the pieces, compute, gather, merge,
and handle failures. That is a lot of plumbing, and you rewrite it for every new problem.

MapReduce says: **you write only two small functions, and the framework does all the
plumbing** (splitting input, running tasks on many machines, moving data, retrying failures).

### 2.2 Key-value pairs

Everything in MapReduce is a **key-value pair**. A key is like a label, and the value is the data.

```
S0      1 20 1        <- key "S0", value "1 20 1"
```

### 2.3 The three phases

```
          input split 1 ──► MAP ──┐
          input split 2 ──► MAP ──┼──► SHUFFLE & SORT ──► REDUCE ──► output
          input split 3 ──► MAP ──┘    (group by key)
```

1. **Map**: runs on each piece of the input separately. It reads records and **emits**
   (prints) key-value pairs. It does not know anything about the other pieces.
2. **Shuffle and sort** (done by the framework, not us): collects all pairs from all mappers
   and **sorts them by key**, so all values with the same key end up next to each other
   and at the same reducer.
3. **Reduce**: receives each key with **all its values** and combines them into the answer
   for that key.

The classic example is word count:

```
map("the cat the")  ->  (the,1) (cat,1) (the,1)
shuffle/sort        ->  (cat,[1])  (the,[1,1])
reduce              ->  (cat,1)    (the,2)
```

### 2.4 The combiner (a "mini reducer")

Without a combiner, word count sends one `(the,1)` pair **per word** across the network.
A **combiner** runs **on the mapper's machine, on that mapper's output only**, and adds up
the values early:

```
mapper output : (the,1) (the,1) (the,1)
combiner      : (the,3)          <- 1 line sent over the network instead of 3
```

Two important rules for a combiner:

- The framework may run it **0, 1, or many times**. So its **output format must be the same as
  its input format**, so it can be fed its own output again.
- The combining operation must give the same answer no matter how values are grouped:
  it must be **associative and commutative**. Sums, counts, min and max are.
  `sum(sum(a,b), c) == sum(a, sum(b,c))`. An average is **not**: the average of averages is wrong.
  That is why we carry **sum and count** and divide only at the very end.

### 2.5 The key concept for us: "partial results that can be merged"

You already used this idea in HW2. In `q8_common.cpp`, every MPI worker built a `Stats`
object for its piece, and the master called `mergingWorkerProcessStats()` to merge them.

MapReduce uses the same idea. We just write the partial stats **as text lines** and let the
framework move them around:

| HW2 MPI | HW3 MapReduce |
|---|---|
| `Stats` struct in memory | a text line `key<TAB>values` |
| `updateStats()` for one record | the mapper prints a partial result for one record |
| `mergingWorkerProcessStats()` | the combiner and reducer merge lines with the same key |
| `MPI_Scatterv` (we split the data) | the framework splits the input file |
| `MPI_Gather` (we collect the results) | the shuffle/sort moves and groups them |
| `printResults()` | the reducer prints the final output |

### 2.6 Why some answers can only be computed at the end

Some values can be merged piece by piece: count, sum, min, max, hottest, coldest.
Two values **need the complete, merged data**:

- **Top K stations**: a station's total count is spread across all input pieces.
- **Busiest interval**: an interval's count is also spread across pieces.

You found this bug yourself in HW2 (the comment in `q8_common.hpp` about keeping a top-K
heap per process). If a piece picks its own "top K" or "busiest" too early, the answer can be
wrong. So in MapReduce we **first merge the per-station and per-interval counts completely,
and only then** pick the top K and the busiest interval, inside the final reducer.

---

## 3. The tools

### 3.1 Hadoop, HDFS, YARN

- **Hadoop** is the most famous open source MapReduce framework (it is written in Java).
- **HDFS** is Hadoop's distributed file system. Big files are stored in blocks on many
  machines, and each block becomes one mapper's input split.
- **YARN** is Hadoop's job scheduler. It decides which machine runs which map or reduce task.

### 3.2 Hadoop Streaming (why we can write C++)

Hadoop is Java, but **Hadoop Streaming** lets the mapper and reducer be *any executable*:

- Hadoop writes input lines to your program's **stdin**
- your program writes `key<TAB>value` lines to **stdout**
- Hadoop treats everything before the first **tab** as the key
- the reducer receives the lines on stdin **already sorted by key**

So our programs are just normal C++ programs that use `cin`/`cout`. No Hadoop library is needed.
A Hadoop run would look roughly like this (for later, if Hadoop gets fixed):

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files mapper,combiner,reducer \
    -mapper ./mapper -combiner ./combiner -reducer ./reducer \
    -numReduceTasks 1 \
    -input /q8/input.txt -output /q8/out
```

### 3.3 Simulating MapReduce with plain Linux tools (important!)

Because every stage is just "stdin → program → stdout", we can build the whole
MapReduce pipeline with a pipe on our laptop, **with no Hadoop at all**:

```bash
./mapper < input.txt | sort | ./combiner | sort | ./reducer
```

| MapReduce phase | Our command |
|---|---|
| map | `./mapper` |
| shuffle/sort | `sort` (puts equal keys next to each other) |
| combine | `./combiner` |
| shuffle/sort again | `sort` |
| reduce | `./reducer` |

This is exactly what the provided `docs/MapreduceForLocalTesting.sh` does (with Python
programs). We will do the same with C++ programs.

> We use `LC_ALL=C sort` so the sort compares plain bytes. Without it, the sort order
> depends on the computer's language settings. For grouping, all that matters is that equal
> keys end up together, but using `LC_ALL=C` makes the runs repeatable and a bit faster.

### 3.4 Slurm: our "distributed" runner (because Hadoop on RCE is broken)

`docs/additional_info.txt` says Hadoop on the RCE cluster currently has an issue, so we should
run MapReduce **using a Slurm script**. The provided `docs/Mapreduce_distributed.sh`
shows how:

```
split input into P chunks        split -d -a 2 -n l/P input chunk_     (l/ = never cut a line)
srun P tasks, each:  mapper   <  chunk_XX  > map_XX.out        } these run in parallel
                     sort        map_XX.out > shuf1_XX.out     } on different
                     combiner <  shuf1_XX.out > comb_XX.out    } nodes/cores
master:              sort comb_*.out > global_shuf2.out      <- the "shuffle" (gather + sort)
master:              reducer < global_shuf2.out > output
```

- `srun` starts the same command on P Slurm tasks. `$SLURM_PROCID` (0..P-1) tells each task
  which chunk it owns.
- This is exactly MapReduce with **P mappers (each followed by a combiner) and 1 reducer**.
- It also times each stage separately, which is useful for the report.

So our three C++ programs will work **unchanged** in three settings: the local pipe, the Slurm
script, and real Hadoop Streaming.

---

## 4. Our design for Q8

### 4.1 The four kinds of keys

For every input line, the mapper prints:

| Key | Value | Why |
|---|---|---|
| `K` | the K from the header | the reducer needs K for top-K. It is printed only for the header line |
| `G` | 21 numbers: the global partial stats (see below) | everything that is "one number for the whole dataset" |
| `S<station_id>` e.g. `S3` | `count temp_sum rain_sum` | per-station totals, needed for TOP_STATIONS |
| `I<interval_id>` e.g. `I2` | `count` | per-60s-interval counts, needed for BUSIEST_INTERVAL (`interval_id = timestamp / 60`) |

The `G` value is these 21 numbers, in this order:

```
count
temp_sum  temp_min  temp_max
hum_sum   hum_min   hum_max
pres_sum  pres_min  pres_max
rain_sum  rain_max
wind_sum  wind_max
extreme_count
hottest_temp  hottest_station  hottest_timestamp
coldest_temp  coldest_station  coldest_timestamp
```

### 4.2 The trick: one record = a partial result with count 1

For a single measurement with temperature 20:
`count=1, temp_sum=20, temp_min=20, temp_max=20, hottest=coldest=this record`.

So **mapper output and combiner output have the same format**. The only thing the
combiner and reducer need to know is **how to merge two lines with the same key**:

| Key | How to merge two values |
|---|---|
| `K` | keep it (all are the same) |
| `G` | add counts and sums; take min of mins and max of maxes; add extreme counts; hottest and coldest are chosen with the HW2 tie-break rules (hotter, then smaller timestamp, then smaller station id) |
| `S<id>` | add all three numbers |
| `I<id>` | add the counts |

Every merge is a sum, min, max or "better of two", so it is associative and commutative.
That means the answer is the same whether the combiner runs 0 or many times, and no matter
how the input was split. This is the correctness argument for the report.

### 4.3 The three programs

| Program | Input | What it does | Output |
|---|---|---|---|
| `mapper` | raw input lines | turns each record into `G`, `S`, `I` lines (and the header into a `K` line) | key-value lines |
| `combiner` | **sorted** key-value lines (from one mapper) | merges consecutive lines with the same key | same format, fewer lines |
| `reducer` | **sorted** key-value lines (from all combiners) | merges lines with the same key, then computes averages, top K and busiest interval | final Q8 output (exact HW2 format) |

### 4.4 Why only one reducer?

The final answer needs a **global** view: top K over *all* stations, busiest over *all*
intervals, and a single `G`. So the last step needs one reducer anyway.

Doesn't one reducer become a bottleneck? After combining, each chunk sends only:
1 `G` line + at most S station lines + one line per distinct interval in that chunk.
That is much less than N records. The `I` lines are the biggest part of this traffic
(interval ids are spread out). This is a good thing to measure and discuss in the report.

### 4.5 Floating-point precision

Values are passed between programs **as text**. If we printed `1013.2500000001` as `1013.25`
at every stage, small errors would add up and the 6-decimal averages might not match
`sequential`. So the mapper and combiner print numbers with **17 significant digits**
(`setprecision(17)`), which is enough to write a `double` as text and read it back exactly.

Adding the same numbers in a **different order** can still change the last few binary digits
of a sum. These differences are around 1e-12 and normally do not show at 6 decimals.
Our correctness tests (diff against `sequential`) will confirm this.

### 4.6 Edge cases

| Case | Behaviour |
|---|---|
| header `0 K S`, no records | only a `K` line reaches the reducer, so it prints `TOTAL_MEASUREMENTS 0` (same as HW2) |
| completely empty file | the reducer gets nothing and prints nothing (same as `sequential`) |
| blank or broken lines | the mapper skips any line that does not have 7 values |
| input split into chunks | only chunk 0 contains the header. That is fine, because the `K` line is still produced once |
| K > number of stations | the reducer prints all the stations that exist (same as HW2) |

---

## 5. Full worked example on `test_sample_input.txt`

Input (header `10 3 4` means N=10, K=3, S=4):

```
10  0 20.0 50.0 1000.0 1.0 4.0
25  1 30.0 60.0 1010.0 2.0 6.0
50  0 40.0 70.0 1020.0 3.0 8.0
70  2 10.0 40.0 990.0  0.0 2.0
90  1 35.0 80.0 1030.0 4.0 10.0
110 0 25.0 55.0 1005.0 2.0 5.0
130 2 0.0  45.0 995.0  5.0 3.0
150 3 45.0 65.0 1015.0 6.0 7.0
170 1 15.0 35.0 985.0  1.0 1.0
190 0 30.0 75.0 1025.0 3.0 9.0
```

Suppose it is split into **2 chunks**: the first 5 records (A) and the last 5 (B).

### Map (chunk A shown, first record)

```
K       3
G       1 20 20 20 50 50 50 1000 1000 1000 1 1 4 4 0 20 0 10 20 0 10
S0      1 20 1
I0      1
...
```

### Combine (per chunk, after sorting)

Let's follow **station 0** and the **intervals**:

```
Chunk A: S0 lines: (1 20 1) (1 40 3)             -> combiner:  S0  2 60 4
         I0 lines: ts 10,25,50                    -> combiner:  I0  3
         I1 lines: ts 70,90                       -> combiner:  I1  2

Chunk B: S0 lines: (1 25 2) (1 30 3)             -> combiner:  S0  2 55 5
         I1: ts 110                               -> combiner:  I1  1
         I2: ts 130,150,170                       -> combiner:  I2  3
         I3: ts 190                               -> combiner:  I3  1
```

### Shuffle and sort (all combiner outputs together)

```
I0 3
I1 2
I1 1
I2 3
I3 1
S0 2 60 4
S0 2 55 5
...
```

### Reduce

```
S0: count 2+2 = 4, temp_sum 60+55 = 115, rain 4+5 = 9
    -> avg temp 115/4 = 28.75                  -> "0 4 28.750000 9.000000"   (matches expected)

I0 = 3, I1 = 2+1 = 3, I2 = 3, I3 = 1
    -> three-way tie at 3, smallest id wins     -> "BUSIEST_INTERVAL 0 3"      (matches expected)
```

Notice the lesson from 2.6: **chunk B alone would say the busiest is I2**, and only I1 looks
like 2 in chunk A. The correct answer appears only after the full merge in the reducer.

---

## 6. The code

### 6.1 `mapper.cpp` (done)

```cpp
ios_base::sync_with_stdio(false);
cin.tie(nullptr);
```
Makes `cin`/`cout` much faster (same as your HW2 code). This matters for millions of lines.

```cpp
cout << setprecision(17);
```
Print doubles with 17 significant digits so nothing is lost (section 4.5).
Note that `20.0` is still printed as `20`, since trailing zeros are not needed.

```cpp
string line;
while (getline(cin, line)) {
```
Read the input **one whole line at a time** until the input ends. We read by line (not
`cin >> N >> K >> S` like HW2) because in MapReduce a mapper might get a chunk **without** the
header line, so we cannot assume the first line is `N K S`.

```cpp
stringstream ss(line);
string tokens[8];
int numTokens = 0;
string word;
while (numTokens < 8 && ss >> word) {
    tokens[numTokens] = word;
    numTokens++;
}
```
`stringstream` lets us use `>>` on a string, just like on `cin`. It splits the line into words
(any amount of spaces works, which is important because the test files have double spaces).
We count the words to decide what kind of line it is.

```cpp
if (numTokens == 3) { cout << "K\t" << tokens[1] << "\n"; continue; }
if (numTokens != 7) continue;
```
3 words means the header, so we print K. Anything other than 7 words is skipped.
`\t` is the **tab** that separates key and value (Hadoop Streaming's rule).

```cpp
long long timestamp = stoll(tokens[0]);
int station_id     = stoi(tokens[1]);
double temperature = stod(tokens[2]);  ...
```
`stoll` / `stoi` / `stod` mean "string to long long / int / double".

Then we print the `G`, `S<id>` and `I<id>` lines described in section 4.1.
`timestamp / 60` is integer division, so it gives the interval id.

### 6.2 `combiner.cpp` (next step)

The main pattern is **"group consecutive equal keys"**, which works because the input is sorted:

```
currentKey = ""
for each line:
    split into key and value
    if key == currentKey:  merge value into the current total
    else:                  print the previous total (if any), start a new total with this value
at the end: print the last total
```

### 6.3 `reducer.cpp` (after that)

Same grouping loop as the combiner, but instead of printing each merged key, it **stores**:
the `G` total, K, a map `station -> (count, temp_sum, rain_sum)`, and a map `interval -> count`.
At the end it computes the averages, picks the top K stations (sorted by count descending,
then station id ascending), picks the busiest interval (largest count, then smallest id), and
prints everything in exactly the HW2 format with 6 decimals.

---

## 7. Testing and benchmarking plan

### Correctness
1. For every file in `../q8/testcases/`, run
   `./mapper < f | LC_ALL=C sort | ./combiner | LC_ALL=C sort | ./reducer`
   and `diff` the result with `../q8/testcases/expected/` (and with `../q8/sequential`).
2. Also test with the input **split into several chunks** (like the Slurm script does) to show
   that the answer does not depend on how the data is split.
3. Test on larger generated datasets (`generate_dataset.py`, fixed seed 42) and compare with `sequential`.

### Benchmarks (for the report)
- Input sizes: e.g. 1M, 10M, 20M, 50M records (the same sizes as the HW2 MPI benchmarks).
- Number of map tasks P: 1, 2, 4, 8.
- With and without the combiner (shows how much data movement the combiner saves).
- Time per stage (map, sort, combine, global sort, reduce), as in the provided script.
- The number of lines/bytes after map vs after combine (data movement).

### MPI vs MapReduce comparison (for the report)
- Execution time and throughput for the same N and number of workers
  (the HW2 numbers are in `../q8/output/mpi_benchmark.log`).
- Where the time goes: MPI spends most of its time reading the input on rank 0 and scattering
  it. In MapReduce each mapper reads its own chunk, but we pay for text output and sorting.
- Data movement: MPI sends binary structs. MapReduce sends text key-value lines.
- Programming effort: in MPI we wrote the split, send, gather and merge ourselves. In
  MapReduce we only write map/combine/reduce, and the framework (or the script) does the rest.
- Flexibility: adding a new statistic in MapReduce means adding one more key type.

---

## 8. Quick glossary

| Term | Meaning |
|---|---|
| key-value pair | `key<TAB>value`, the unit of data in MapReduce |
| input split / chunk | the piece of the input that one mapper reads |
| mapper | turns input records into key-value pairs |
| shuffle and sort | groups all pairs by key (done by the framework, or by `sort` for us) |
| combiner | local mini-reducer on the mapper side that reduces data movement |
| reducer | merges all values of a key into the final result |
| associative / commutative | grouping and order don't change the result, which is what lets combining happen anywhere |
| Hadoop Streaming | lets any stdin/stdout program act as a mapper or reducer |
| YARN | Hadoop's scheduler that places tasks on machines |
| HDFS | Hadoop's distributed file system |
| Slurm / `srun` | cluster job scheduler. `srun` runs a command on many tasks in parallel |
