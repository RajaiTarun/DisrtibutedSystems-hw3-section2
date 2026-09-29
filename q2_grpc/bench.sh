#!/bin/bash
#SBATCH --job-name=q8-grpc-bench
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=2G
#SBATCH --time=02:00:00
#SBATCH --output=bench_%j.log
#SBATCH --error=bench_%j.err

# benchmarks of the streaming system. runs on the cluster (sbatch) or on one machine (./bench.sh)
#
# placement on the cluster (4 nodes):
#   node 1: coordinator          node 2 and 3: workers (alternating)          node 4: client + dashboards
# on one machine everything runs on localhost
#
# experiments (every run starts a fresh system and checks its final result against HW2's sequential):
#   workers   W = 1, 2, 4, 8, both strategies, batch 1000, one dashboard querying every 100 ms   (N_MAIN)
#   batch     B = 1, 10, 100, 1000, 10000, W = 4, interval                                     (N_BATCH)
#   queries   Q = 0, 1, 4, 16 dashboards querying back to back, W = 4, interval, batch 1000     (N_MAIN)
#   rate      R = 200K, 400K, 800K, 1.6M records/s and max speed, W = 4, interval, batch 1000   (N_MAIN)
#
# results: results/bench_results.csv (one line per run), logs in results/bench_logs/
#
# usage:
#   sbatch bench.sh                               (cluster; run ./setup_env.sh once before)
#   ./bench.sh                                    (one machine)
#   N_MAIN=1000000 N_BATCH=50000 ./bench.sh       (smaller, for a quick test)
#   EXPERIMENTS="workers batch" ./bench.sh        (only some experiments)

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "$0")"
fi
PY=$(pwd)/.venv/bin/python
if [ ! -x "$PY" ]; then
    echo "run ./setup_env.sh first"
    exit 1
fi
[ -f weather_pb2.py ] || ./gen_proto.sh > /dev/null

N_MAIN=${N_MAIN:-5000000}
N_BATCH=${N_BATCH:-200000}
EXPERIMENTS=${EXPERIMENTS:-"workers batch queries rate"}
# ports: different users on the same node must not use the same port, so the default depends on the user id
BASE_PORT=${BASE_PORT:-$((40000 + $(id -u) % 20000))}

WORK=$(pwd)/bench_work_${SLURM_JOB_ID:-local}
mkdir -p "$WORK" results/bench_logs
CSV=results/bench_results.csv
echo "experiment,N,workers,strategy,batch,rate,query_clients,send_s,total_s,throughput,queries,q_mean_ms,q_p50_ms,q_p95_ms,q_max_ms,records_per_worker,coord_rss_mb,coord_cpu_s,worker_rss_mb_max,worker_cpu_s_total,correct" > "$CSV"

# ---------------- where things run ----------------
if [ -n "$SLURM_JOB_ID" ]; then
    NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
    NUM_NODES=${#NODES[@]}
    COORD_NODE=${NODES[0]}
    CLIENT_NODE=${NODES[$((NUM_NODES - 1))]}
    if [ "$NUM_NODES" -ge 3 ]; then
        WORKER_NODES=("${NODES[@]:1:$((NUM_NODES - 2))}")
    else
        WORKER_NODES=("${NODES[0]}")
    fi
    CPUS=${SLURM_CPUS_PER_TASK:-1}
else
    COORD_NODE=127.0.0.1
    CLIENT_NODE=127.0.0.1
    WORKER_NODES=(127.0.0.1)
fi

# runs a command on a node in the background (srun on the cluster, directly on one machine)
launch() {
    local node=$1
    shift
    if [ -n "$SLURM_JOB_ID" ]; then
        srun --overlap -N1 -n1 -c "$CPUS" -w "$node" "$@" &
    else
        "$@" &
    fi
}
# runs a command on a node and waits for it
run_on() {
    local node=$1
    shift
    if [ -n "$SLURM_JOB_ID" ]; then
        srun --overlap -N1 -n1 -c "$CPUS" -w "$node" "$@"
    else
        "$@"
    fi
}

# GNU time measures memory and cpu time (not available on a mac)
if /usr/bin/time -f "%M" true > /dev/null 2>&1; then TIME_OK=1; else TIME_OK=0; fi

echo "========================================="
echo "Job id:       ${SLURM_JOB_ID:-local}"
echo "Python:       $("$PY" --version)"
echo "Coordinator:  $COORD_NODE"
echo "Workers on:   ${WORKER_NODES[*]}"
echo "Client on:    $CLIENT_NODE"
echo "Port base:    $BASE_PORT"
echo "Experiments:  $EXPERIMENTS   (N_MAIN=$N_MAIN, N_BATCH=$N_BATCH)"
echo "========================================="

# ---------------- datasets + reference outputs (HW2 C++ sequential) ----------------
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o "$WORK/sequential" || exit 1
for N in $N_MAIN $N_BATCH; do
    if [ ! -f "$WORK/data_$N.txt" ]; then
        "$PY" ../q8/generate_dataset.py --n "$N" --k 10 --s 100 --seed 42 --out "$WORK/data_$N.txt" > /dev/null
        "$WORK/sequential" < "$WORK/data_$N.txt" > "$WORK/expected_$N.txt" 2> /dev/null
    fi
done

COORD="$COORD_NODE:$BASE_PORT"

# reads "key=value" out of a line
field() { echo "$2" | tr ' ' '\n' | grep "^$1=" | cut -d= -f2; }

# ---------------- one run ----------------
# $1 experiment  $2 N  $3 workers  $4 strategy  $5 batch  $6 rate  $7 query clients  $8 query interval ms
run_one() {
    local EXP=$1 N=$2 W=$3 STRAT=$4 BATCH=$5 RATE=$6 Q=$7 QINT=$8
    local LOG=results/bench_logs/${EXP}_N${N}_W${W}_${STRAT}_B${BATCH}_R${RATE}_Q${Q}
    rm -f "$WORK"/time_*.txt
    local PIDS=()

    # workers
    local ADDRESSES=()
    for ((i = 0; i < W; i++)); do
        local NODE=${WORKER_NODES[$((i % ${#WORKER_NODES[@]}))]}
        local PORT=$((BASE_PORT + 1 + i))
        if [ $TIME_OK -eq 1 ]; then
            launch "$NODE" /usr/bin/time -f "%e %M %U %S" -o "$WORK/time_worker_$i.txt" "$PY" worker.py "0.0.0.0:$PORT" 2> /dev/null
        else
            launch "$NODE" "$PY" worker.py "0.0.0.0:$PORT" 2> /dev/null
        fi
        PIDS+=($!)
        ADDRESSES+=("$NODE:$PORT")
    done

    # coordinator (waits until all workers are reachable)
    if [ $TIME_OK -eq 1 ]; then
        launch "$COORD_NODE" /usr/bin/time -f "%e %M %U %S" -o "$WORK/time_coord.txt" "$PY" coordinator.py "0.0.0.0:$BASE_PORT" "$STRAT" "${ADDRESSES[@]}" 2> "$LOG.coord.log"
    else
        launch "$COORD_NODE" "$PY" coordinator.py "0.0.0.0:$BASE_PORT" "$STRAT" "${ADDRESSES[@]}" 2> "$LOG.coord.log"
    fi
    PIDS+=($!)

    # query dashboards (they wait for the coordinator and query until the stream is done)
    local QPIDS=()
    for ((q = 0; q < Q; q++)); do
        launch "$CLIENT_NODE" "$PY" dashboard.py "$COORD" --bench --interval "$QINT" > "$LOG.queries_$q.txt" 2> /dev/null
        QPIDS+=($!)
    done

    # the client streams the dataset
    run_on "$CLIENT_NODE" "$PY" client.py "$COORD" "$WORK/data_$N.txt" "$BATCH" "$RATE" > "$LOG.client.txt" 2> "$LOG.client.log"
    for pid in "${QPIDS[@]}"; do wait "$pid"; done

    # final result + summary, then stop the system
    "$PY" dashboard.py "$COORD" --once > "$LOG.output.txt" 2> /dev/null
    "$PY" dashboard.py "$COORD" --stats > "$LOG.stats.txt" 2> /dev/null
    "$PY" dashboard.py "$COORD" --shutdown 2> /dev/null
    for pid in "${PIDS[@]}"; do wait "$pid" 2> /dev/null; done

    # ---- collect the numbers ----
    local CORRECT RES SEND_S TOTAL_S THROUGHPUT PER_WORKER
    CORRECT=$("$PY" ../q1_mapreduce/compare_outputs.py "$WORK/expected_$N.txt" "$LOG.output.txt" 2> /dev/null)
    RES=$(grep RESULT "$LOG.client.txt")
    SEND_S=$(field send_s "$RES")
    TOTAL_S=$(field total_s "$RES")
    THROUGHPUT=$(field throughput "$RES")
    PER_WORKER=$(field per_worker "$(cat "$LOG.stats.txt")")

    # query latencies of all dashboards together (count = sum, mean/p50/p95 = average, max = max)
    local QSTATS=",,,,"
    if [ "$Q" -gt 0 ]; then
        QSTATS=$(cat "$LOG".queries_*.txt | "$PY" -c "
import sys
rows = [dict(kv.split('=') for kv in line.split()[1:]) for line in sys.stdin if line.startswith('QUERIES')]
if rows:
    n = sum(int(r['count']) for r in rows)
    avg = lambda k: sum(float(r[k]) for r in rows) / len(rows)
    print(f\"{n},{avg('mean_ms'):.3f},{avg('p50_ms'):.3f},{avg('p95_ms'):.3f},{max(float(r['max_ms']) for r in rows):.3f}\")
else:
    print(',,,,')")
    fi

    # memory / cpu from GNU time: "elapsed max_rss_kb user_s sys_s" (last line of each file)
    local MEM=",,,"
    if [ $TIME_OK -eq 1 ] && [ -f "$WORK/time_coord.txt" ]; then
        MEM=$("$PY" -c "
import glob
def read(f):
    e, m, u, s = open(f).read().strip().split('\n')[-1].split()
    return float(m) / 1024, float(u) + float(s)
c = read('$WORK/time_coord.txt')
w = [read(f) for f in glob.glob('$WORK/time_worker_*.txt')]
print(f'{c[0]:.1f},{c[1]:.2f},{max(x[0] for x in w):.1f},{sum(x[1] for x in w):.2f}')" 2> /dev/null || echo ",,,")
    fi

    echo "$EXP,$N,$W,$STRAT,$BATCH,$RATE,$Q,$SEND_S,$TOTAL_S,$THROUGHPUT,$QSTATS,$PER_WORKER,$MEM,${CORRECT:-FAIL}" >> "$CSV"
    printf "  %-9s N=%-8s W=%-2s %-10s B=%-6s R=%-8s Q=%-3s -> %12s rec/s  total %8ss  %s\n" \
        "$EXP" "$N" "$W" "$STRAT" "$BATCH" "$RATE" "$Q" "$THROUGHPUT" "$TOTAL_S" "${CORRECT:-FAIL}"
}

# ---------------- experiments ----------------
for EXP in $EXPERIMENTS; do
    echo ""
    echo "=== experiment: $EXP ==="
    case $EXP in
        workers)
            for STRAT in interval roundrobin; do
                for W in 1 2 4 8; do run_one workers "$N_MAIN" $W $STRAT 1000 0 1 100; done
            done ;;
        batch)
            for B in 1 10 100 1000 10000; do run_one batch "$N_BATCH" 4 interval $B 0 0 0; done ;;
        queries)
            for Q in 0 1 4 16; do run_one queries "$N_MAIN" 4 interval 1000 0 $Q 0; done ;;
        rate)
            for R in 200000 400000 800000 1600000 0; do run_one rate "$N_MAIN" 4 interval 1000 $R 0 0; done ;;
    esac
done

rm -rf "$WORK"
echo ""
echo "========================================="
echo "Benchmark complete. Results in $CSV"
echo "Wrong results: $(grep -c ',FAIL$' "$CSV")"
echo "========================================="
