#!/usr/bin/env bash
cd "$(dirname "$0")"

echo "=== Building sequential ==="
g++ -std=c++17 -O2 -Wall sequential.cpp q8_common.cpp -o sequential || { echo "sequential build failed"; exit 1; }

echo "=== Building MPI ==="
mpicxx -std=c++17 -O2 -Wall mpi.cpp q8_common.cpp -o mpi_q8 || { echo "mpi build failed"; exit 1; }

PASS=0
FAIL=0

echo ""
echo "=== Step 1: sequential output vs hand-verified expected output ==="

for expected in testcases/expected/*.txt; do
    name=$(basename "$expected")
    input="testcases/$name"
    ./sequential < "$input" > /tmp/q8_verify_seq.out
    if diff -q "$expected" /tmp/q8_verify_seq.out > /dev/null; then
        PASS=$((PASS+1))
        echo "PASS  $name"
    else
        FAIL=$((FAIL+1))
        echo "FAIL  $name"
        diff "$expected" /tmp/q8_verify_seq.out
    fi
done

echo ""
echo "=== Step 2: MPI output vs sequential output, multiple process counts ==="

TESTCASES=(testcases/test_*.txt testcases/data_100.txt testcases/data_1K.txt testcases/data_10K.txt)
PROCESS_COUNTS=(2 4 9)

for input in "${TESTCASES[@]}"; do
    ./sequential < "$input" > /tmp/q8_verify_seq.out
    for p in "${PROCESS_COUNTS[@]}"; do
        mpirun --oversubscribe -np "$p" ./mpi_q8 < "$input" 2>/dev/null > /tmp/q8_verify_mpi.out
        if diff -q /tmp/q8_verify_seq.out /tmp/q8_verify_mpi.out > /dev/null; then
            PASS=$((PASS+1))
        else
            FAIL=$((FAIL+1))
            echo "FAIL  $(basename "$input") (P=$p)"
            diff /tmp/q8_verify_seq.out /tmp/q8_verify_mpi.out
        fi
    done
done

echo ""
echo "=== Summary: $PASS passed, $FAIL failed ==="

if [ "$FAIL" -ne 0 ]; then
    echo "VERIFICATION: FAILED"
    exit 1
fi

echo "VERIFICATION: PASSED"
