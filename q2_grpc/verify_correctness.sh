#!/bin/bash
# correctness check for the streaming system (runs on one machine)
#
# test 0: our python analytics (sequential.py) vs HW2's C++ sequential program, all test cases
# test 1: every HW2 test case through the gRPC system, with 1, 2 and 4 workers, both strategies and
#         batch sizes 1, 7 and 1000. the final analytics (dashboard --once) must match HW2's sequential
# test 2: generated 100K and 1M record datasets (seed 42), 4 workers, both strategies
# test 3: queries during the stream: 4 dashboards query non-stop while 1M records stream in.
#         no query may fail, the processed count must never go down and never exceed N,
#         and the final result must match
#
# outputs are compared with ../q1_mapreduce/compare_outputs.py:
#   EXACT = identical, FP_CLOSE = only floating point rounding in the last digits of big sums
#
# usage: ./verify_correctness.sh

cd "$(dirname "$0")"
PY=$(pwd)/.venv/bin/python
if [ ! -x "$PY" ]; then
    echo "run ./setup_env.sh first"
    exit 1
fi
[ -f weather_pb2.py ] || ./gen_proto.sh > /dev/null

# per-user port, so we never clash with another user's server on the same node
BASE_PORT=${BASE_PORT:-$((40000 + $(id -u) % 20000))}
COORD="127.0.0.1:$BASE_PORT"
TESTCASES=../q8/testcases
COMPARE=../q1_mapreduce/compare_outputs.py
TMP=$(mktemp -d)

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

PIDS=()
start_system() {    # $1 = number of workers, $2 = strategy
    PIDS=()
    local addresses=()
    for ((i = 0; i < $1; i++)); do
        "$PY" worker.py "127.0.0.1:$((BASE_PORT + 1 + i))" 2> /dev/null &
        PIDS+=($!)
        addresses+=("127.0.0.1:$((BASE_PORT + 1 + i))")
    done
    "$PY" coordinator.py "$COORD" "$2" "${addresses[@]}" 2> "$TMP/coordinator.log" &
    PIDS+=($!)
    wait_for_coordinator "$TMP/coordinator.log" $!
}
stop_system() {
    GRPC_CONNECT_TIMEOUT=5 "$PY" dashboard.py "$COORD" --shutdown 2> /dev/null
    sleep 0.5
    for pid in "${PIDS[@]}"; do kill "$pid" 2> /dev/null; done
    wait 2> /dev/null
}
cleanup() {
    stop_system
    rm -rf "$TMP"
}
trap cleanup EXIT

echo "=== Building HW2 sequential (reference) ==="
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o "$TMP/sequential" || exit 1

PASS=0
FAIL=0
record() {   # $1 = result word, $2 = label, $3 = file with details
    if [ "$1" = "FAIL" ] || [ -z "$1" ]; then
        FAIL=$((FAIL + 1))
        echo "FAIL   $2"
        [ -n "$3" ] && cat "$3"
    else
        PASS=$((PASS + 1))
        echo "$1  $2"
    fi
}

echo ""
echo "=== Test 0: python analytics (sequential.py) vs HW2 C++ sequential ==="
for input in "$TESTCASES"/test_*.txt; do
    "$TMP/sequential" < "$input" > "$TMP/expected.txt" 2> /dev/null
    "$PY" sequential.py "$input" > "$TMP/actual.txt"
    record "$("$PY" "$COMPARE" "$TMP/expected.txt" "$TMP/actual.txt" 2> "$TMP/diff.txt")" "sequential.py $(basename "$input")" "$TMP/diff.txt"
done

# streams one file and compares the final analytics with sequential. $1 = input, $2 = batch, $3 = label
check_one() {
    "$TMP/sequential" < "$1" > "$TMP/expected.txt" 2> /dev/null
    "$PY" client.py "$COORD" "$1" "$2" 0 > /dev/null 2> "$TMP/client.log"
    "$PY" dashboard.py "$COORD" --once > "$TMP/actual.txt" 2> /dev/null
    cat "$TMP/client.log" >> "$TMP/diff.txt"
    record "$("$PY" "$COMPARE" "$TMP/expected.txt" "$TMP/actual.txt" 2> "$TMP/diff.txt")" "$3" "$TMP/diff.txt"
}

echo ""
echo "=== Test 1: HW2 test cases x workers x strategy x batch size ==="
for STRATEGY in interval roundrobin; do
    for W in 1 2 4; do
        start_system $W $STRATEGY
        for input in "$TESTCASES"/test_*.txt; do
            for BATCH in 1 7 1000; do
                check_one "$input" $BATCH "$(basename "$input") W=$W $STRATEGY batch=$BATCH"
            done
        done
        stop_system
    done
done

echo ""
echo "=== Test 2: generated datasets ==="
"$PY" ../q8/generate_dataset.py --n 100000 --k 10 --s 100 --seed 42 --out "$TMP/data_100K.txt" > /dev/null
"$PY" ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out "$TMP/data_1M.txt" > /dev/null
for STRATEGY in interval roundrobin; do
    start_system 4 $STRATEGY
    check_one "$TMP/data_100K.txt" 1000 "data_100K W=4 $STRATEGY batch=1000"
    check_one "$TMP/data_1M.txt" 1000 "data_1M W=4 $STRATEGY batch=1000"
    stop_system
done

echo ""
echo "=== Test 3: 4 dashboards query non-stop while 1M records stream in ==="
start_system 4 interval
Q_PIDS=()
for q in 1 2 3 4; do
    # --bench queries until the stream is done and reports the latencies, the highest processed count
    # it saw, and whether that count ever went down (it must not)
    "$PY" dashboard.py "$COORD" --bench > "$TMP/queries_$q.txt" 2> "$TMP/queries_$q.err" &
    Q_PIDS+=($!)
done
check_one "$TMP/data_1M.txt" 100 "data_1M W=4 interval batch=100, with 4 dashboards querying"
QUERY_FAIL=0
for q in 1 2 3 4; do
    wait "${Q_PIDS[$((q - 1))]}" || QUERY_FAIL=1
    [ -s "$TMP/queries_$q.err" ] && QUERY_FAIL=1
    grep -q "monotonic=yes" "$TMP/queries_$q.txt" || QUERY_FAIL=1
    MAXP=$(sed -E 's/.*max_processed=([0-9]+).*/\1/' "$TMP/queries_$q.txt")
    [ "${MAXP:-0}" -gt 1000000 ] && QUERY_FAIL=1
    echo "       dashboard $q: $(cat "$TMP/queries_$q.txt")"
done
if [ $QUERY_FAIL -eq 0 ]; then
    record PASS "all concurrent queries succeeded, counts never went down and never exceeded N"
else
    cat "$TMP"/queries_*.err
    record FAIL "some concurrent queries failed"
fi
stop_system

echo ""
echo "=== Summary: $PASS passed, $FAIL failed ==="
if [ $FAIL -ne 0 ]; then
    echo "VERIFICATION: FAILED"
    exit 1
fi
echo "VERIFICATION: PASSED"
