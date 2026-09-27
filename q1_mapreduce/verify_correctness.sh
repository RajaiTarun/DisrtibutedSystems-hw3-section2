#!/bin/bash
# correctness check for the q8 mapreduce implementation (runs locally, no hadoop, no slurm needed)
#
# test 1: mapreduce output vs the hand verified expected outputs of hw2 (../q8/testcases/expected)
# test 2: mapreduce output vs hw2's sequential program, with the input split into P chunks
#         (1 chunk = 1 mapper). this is what the slurm script does on the cluster, so this checks
#         that the answer does not depend on how the input is split
#
# usage: ./verify_correctness.sh

cd "$(dirname "$0")"

TESTCASE_DIR=../q8/testcases
TMP_DIR=$(mktemp -d)
# delete the temporary folder when the script ends
trap 'rm -rf "$TMP_DIR"' EXIT

echo "=== Building ==="
g++ -std=c++17 -O2 -Wall mapper.cpp -o mapper || exit 1
g++ -std=c++17 -O2 -Wall combiner.cpp -o combiner || exit 1
g++ -std=c++17 -O2 -Wall reducer.cpp -o reducer || exit 1
# hw2's sequential program is our correctness reference
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o "$TMP_DIR/sequential" || exit 1

PASS=0
FAIL=0

echo ""
echo "=== Test 1: mapreduce output vs expected output ==="

for expected in "$TESTCASE_DIR"/expected/*.txt; do
    name=$(basename "$expected")
    ./mapper < "$TESTCASE_DIR/$name" | LC_ALL=C sort | ./combiner | LC_ALL=C sort | ./reducer > "$TMP_DIR/mr.out"

    if diff -q "$expected" "$TMP_DIR/mr.out" > /dev/null; then
        PASS=$((PASS+1))
        echo "PASS  $name"
    else
        FAIL=$((FAIL+1))
        echo "FAIL  $name"
        diff "$expected" "$TMP_DIR/mr.out"
    fi
done

echo ""
echo "=== Test 2: mapreduce output vs sequential, input split into P chunks ==="

# the small testcases + 2 bigger generated datasets (fixed seed, so always the same data)
python3 ../q8/generate_dataset.py --n 100000 --k 10 --s 100 --seed 42 --out "$TMP_DIR/data_100K.txt" > /dev/null
python3 ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out "$TMP_DIR/data_1M.txt" > /dev/null

INPUTS=("$TESTCASE_DIR"/test_*.txt "$TMP_DIR/data_100K.txt" "$TMP_DIR/data_1M.txt")
CHUNK_COUNTS=(1 2 4 8)

for input in "${INPUTS[@]}"; do
    "$TMP_DIR/sequential" < "$input" > "$TMP_DIR/seq.out" 2> /dev/null

    for P in "${CHUNK_COUNTS[@]}"; do
        rm -f "$TMP_DIR"/chunk_*

        # split into P chunks by number of lines
        # (the slurm script uses "split -n l/P", but that option does not exist on macOS, so here we
        # calculate lines per chunk ourselves. +1 in case the last line has no newline)
        lines=$(( $(wc -l < "$input") + 1 ))
        split -a 2 -l $(( (lines + P - 1) / P )) "$input" "$TMP_DIR/chunk_"

        # map + sort + combine on every chunk separately (like separate mapper tasks)
        for chunk in "$TMP_DIR"/chunk_??; do
            [ -f "$chunk" ] || continue   # an empty input file creates no chunks
            ./mapper < "$chunk" | LC_ALL=C sort | ./combiner > "$chunk.comb"
        done

        # global sort of all combiner outputs + 1 reducer
        cat "$TMP_DIR"/chunk_*.comb 2> /dev/null | LC_ALL=C sort | ./reducer > "$TMP_DIR/mr.out"

        if diff -q "$TMP_DIR/seq.out" "$TMP_DIR/mr.out" > /dev/null; then
            PASS=$((PASS+1))
            echo "PASS  $(basename "$input") (P=$P)"
        else
            FAIL=$((FAIL+1))
            echo "FAIL  $(basename "$input") (P=$P)"
            diff "$TMP_DIR/seq.out" "$TMP_DIR/mr.out"
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
