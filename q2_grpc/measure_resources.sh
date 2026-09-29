#!/bin/bash
# measures the peak memory and cpu time of every process of one streaming run (one machine)
#
# run: 4 workers, interval strategy, batch 1000, one dashboard querying every 100 ms, N records
# every process is started through run_measured.py, which records its peak memory (max RSS) and
# its cpu time (user + system) when it exits
#
# usage: ./measure_resources.sh [N] [workers] [strategy]      (default 5000000 4 interval)
# output: results/resources_<strategy>.txt (one line per process) and a summary on the screen

cd "$(dirname "$0")"
PY=$(pwd)/.venv/bin/python
N=${1:-5000000}
W=${2:-4}
STRATEGY=${3:-interval}
BASE_PORT=${BASE_PORT:-$((40000 + $(id -u) % 20000))}
COORD="127.0.0.1:$BASE_PORT"
TMP=$(mktemp -d)
mkdir -p results

"$PY" ../q8/generate_dataset.py --n "$N" --k 10 --s 100 --seed 42 --out "$TMP/data.txt" > /dev/null

PIDS=()
ADDRESSES=()
for ((i = 0; i < W; i++)); do
    "$PY" run_measured.py "$TMP/worker_$i.txt" "$PY" worker.py "127.0.0.1:$((BASE_PORT + 1 + i))" 2> /dev/null &
    PIDS+=($!)
    ADDRESSES+=("127.0.0.1:$((BASE_PORT + 1 + i))")
done
"$PY" run_measured.py "$TMP/coordinator.txt" "$PY" coordinator.py "$COORD" "$STRATEGY" "${ADDRESSES[@]}" 2> /dev/null &
PIDS+=($!)

"$PY" run_measured.py "$TMP/dashboard.txt" "$PY" dashboard.py "$COORD" --bench --interval 100 > "$TMP/queries.out" &
DASH=$!
"$PY" run_measured.py "$TMP/client.txt" "$PY" client.py "$COORD" "$TMP/data.txt" 1000 0 > "$TMP/client.out" 2> /dev/null
wait $DASH
GRPC_CONNECT_TIMEOUT=5 "$PY" dashboard.py "$COORD" --shutdown 2> /dev/null
for pid in "${PIDS[@]}"; do wait "$pid"; done

OUT=results/resources_$STRATEGY.txt
{
    echo "# N=$N workers=$W strategy=$STRATEGY batch=1000, 1 dashboard every 100 ms"
    grep RESULT "$TMP/client.out"
    cat "$TMP/queries.out"
    cat "$TMP/coordinator.txt" "$TMP"/worker_*.txt "$TMP/client.txt" "$TMP/dashboard.txt"
} > "$OUT"
cat "$OUT"
rm -rf "$TMP"
