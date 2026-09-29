#!/bin/bash
# runs the whole streaming system on this machine and shows the live dashboard while the data streams
#
# usage: ./run_local.sh <input_file> [workers] [strategy] [batch_size] [rate]
#   workers    number of worker processes          (default 4)
#   strategy   interval | roundrobin               (default interval)
#   batch_size records per gRPC message            (default 1000)
#   rate       records per second, 0 = max speed   (default 0)
#
# example (slow stream, so the dashboard has time to show the numbers growing):
#   .venv/bin/python ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out data_1M.txt
#   ./run_local.sh data_1M.txt 4 interval 1000 100000
#
# what it does:
#   1. starts W workers (ports BASE_PORT+1 ...) and the coordinator (port BASE_PORT)
#   2. starts the client in the background, and the live dashboard in the foreground
#   3. prints the final result and compares it with HW2's sequential program
#   4. stops everything

cd "$(dirname "$0")"
PY=$(pwd)/.venv/bin/python
if [ ! -x "$PY" ]; then
    echo "run ./setup_env.sh first"
    exit 1
fi
[ -f weather_pb2.py ] || ./gen_proto.sh > /dev/null

INPUT=$1
W=${2:-4}
STRATEGY=${3:-interval}
BATCH=${4:-1000}
RATE=${5:-0}
# per-user port, so we never clash with another user's server on the same node
BASE_PORT=${BASE_PORT:-$((40000 + $(id -u) % 20000))}
COORD="127.0.0.1:$BASE_PORT"

if [ -z "$INPUT" ] || [ ! -f "$INPUT" ]; then
    echo "usage: ./run_local.sh <input_file> [workers] [strategy] [batch_size] [rate]"
    exit 1
fi
mkdir -p logs

# waits until the coordinator answers; if it does not start (e.g. its port is taken by another
# program), stop with a clear message instead of letting every test wait for a timeout
wait_for_coordinator() {   # $1 = coordinator log file, $2 = coordinator process id
    for ((try = 0; try < 120; try++)); do
        if "$PY" -c "
import sys, grpc
from grpc_common import CHANNEL_OPTIONS
grpc.channel_ready_future(grpc.insecure_channel(sys.argv[1], options=CHANNEL_OPTIONS)).result(timeout=1)
" "$COORD" 2> /dev/null; then
            return 0
        fi
        kill -0 "$2" 2> /dev/null || break    # the coordinator has already stopped: no need to wait
    done
    echo "ERROR: the coordinator did not start on $COORD (port in use? try BASE_PORT=45000 $0)"
    echo "--- coordinator log ---"
    cat "$1"
    exit 1
}

# stop everything when the script ends (also on Ctrl+C)
PIDS=()
cleanup() {
    GRPC_CONNECT_TIMEOUT=5 "$PY" dashboard.py "$COORD" --shutdown 2> /dev/null
    sleep 0.5
    for pid in "${PIDS[@]}"; do kill "$pid" 2> /dev/null; done
    wait 2> /dev/null
}
trap cleanup EXIT

# 1. workers + coordinator (the coordinator waits until every worker is reachable)
WORKER_ADDRESSES=()
for ((i = 0; i < W; i++)); do
    PORT=$((BASE_PORT + 1 + i))
    "$PY" worker.py "127.0.0.1:$PORT" 2> "logs/worker_$i.log" &
    PIDS+=($!)
    WORKER_ADDRESSES+=("127.0.0.1:$PORT")
done
"$PY" coordinator.py "$COORD" "$STRATEGY" "${WORKER_ADDRESSES[@]}" 2> logs/coordinator.log &
PIDS+=($!)
wait_for_coordinator logs/coordinator.log $!

# 2. client in the background, live dashboard in the foreground until the stream is done
"$PY" client.py "$COORD" "$INPUT" "$BATCH" "$RATE" > logs/client.out 2> logs/client.log &
CLIENT_PID=$!
"$PY" dashboard.py "$COORD" --interval 300 --exit-when-done
wait $CLIENT_PID

# 3. summary + correctness check
echo ""
echo "=== client ==="
tail -1 logs/client.log
"$PY" dashboard.py "$COORD" --once > logs/final_output.txt
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o logs/sequential 2> /dev/null
logs/sequential < "$INPUT" > logs/sequential_output.txt 2> /dev/null
echo "=== final result vs HW2 sequential: $("$PY" ../q1_mapreduce/compare_outputs.py logs/sequential_output.txt logs/final_output.txt 2> /dev/null) ==="
echo "final output saved in logs/final_output.txt"
