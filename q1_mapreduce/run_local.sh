#!/bin/bash
# runs the q8 mapreduce pipeline on one input file, on the local machine (no hadoop, no slurm)
# same idea as docs/MapreduceForLocalTesting.sh, just with our c++ programs
#
# usage: ./run_local.sh <input_file> [output_file]
#   if output_file is not given, the result is printed on the screen

cd "$(dirname "$0")"

INPUT_FILE=$1
OUTPUT_FILE=$2

if [ -z "$INPUT_FILE" ] || [ ! -f "$INPUT_FILE" ]; then
    echo "usage: ./run_local.sh <input_file> [output_file]"
    exit 1
fi

# step 0: compile the 3 programs
g++ -std=c++17 -O2 -Wall mapper.cpp -o mapper || exit 1
g++ -std=c++17 -O2 -Wall combiner.cpp -o combiner || exit 1
g++ -std=c++17 -O2 -Wall reducer.cpp -o reducer || exit 1

# LC_ALL=C makes sort compare plain bytes, so equal keys always end up next to each other
# (without it, sort can ignore the tab and mix up keys like S1 and S10)
#
# step 1: mapper
# step 2: sort (like hadoop's shuffle)
# step 3: combiner
# step 4: sort again
# step 5: reducer
if [ -z "$OUTPUT_FILE" ]; then
    ./mapper < "$INPUT_FILE" | LC_ALL=C sort | ./combiner | LC_ALL=C sort | ./reducer
else
    ./mapper < "$INPUT_FILE" | LC_ALL=C sort | ./combiner | LC_ALL=C sort | ./reducer > "$OUTPUT_FILE"
    echo "MapReduce pipeline completed. Output saved to $OUTPUT_FILE"
fi
