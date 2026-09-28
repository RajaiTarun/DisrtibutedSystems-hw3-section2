#!/bin/bash
#SBATCH --job-name=q8-mapreduce
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=00:30:00
#SBATCH --output=%j.log
#SBATCH --error=%j.err

# runs the q8 mapreduce pipeline on the cluster using slurm (because hadoop on rce is not working right now)
# based on docs/Mapreduce_distributed.sh that was given to us
#
#   split input into P chunks                          (P = number of slurm tasks = number of mappers)
#   srun P tasks in parallel:  mapper   < chunk_XX     > map_XX.out
#                              sort       map_XX.out   > sort_XX.out
#                              combiner < sort_XX.out  > comb_XX.out
#   master:                    sort -m comb_*.out      > shuffled.out   (the "shuffle": gather + sort)
#   master:                    reducer  < shuffled.out > output
#
# at the end it also runs hw2's sequential program on the same input and compares the two outputs
#
# usage (run it from inside the q1_mapreduce folder, and ../q8 must also be there):
#   sbatch run_slurm.sh <input_file>                  -> 4 mappers (default)
#   sbatch --ntasks=8 run_slurm.sh <input_file>       -> 8 mappers
#   sbatch --ntasks=8 --nodes=4 run_slurm.sh <input>  -> 8 mappers spread over 4 nodes
# if no input file is given, it uses ../q8/testcases/test_sample_input.txt
# the q8 output goes to output/mr_<input name>, the timings go to <jobid>.log

# slurm starts the script in the folder where we ran sbatch
if [ -n "$SLURM_SUBMIT_DIR" ]; then
    cd "$SLURM_SUBMIT_DIR"
fi

INPUT_FILE=${1:-../q8/testcases/test_sample_input.txt}
if [ ! -f "$INPUT_FILE" ]; then
    echo "Input file not found: $INPUT_FILE"
    exit 1
fi

# number of mappers = number of slurm tasks (4 if we are not inside slurm)
P=${SLURM_NTASKS:-4}

# all the temporary files go here. it must be inside our home folder (not /tmp),
# because the srun tasks can run on different nodes and they all need to see these files
WORK=$(pwd)/work_${SLURM_JOB_ID:-local}
rm -rf "$WORK"
mkdir -p "$WORK" output
# the srun tasks need to know WORK, so we export it
export WORK

OUTPUT_FILE=output/mr_$(basename "$INPUT_FILE")

echo "========================================="
echo "Job id:    $SLURM_JOB_ID"
echo "Nodes:     $SLURM_JOB_NODELIST"
echo "Mappers:   $P"
echo "Input:     $INPUT_FILE ($(wc -l < "$INPUT_FILE") lines)"
echo "========================================="

# compile on the cluster itself (binaries built on a mac do not run on linux)
echo "Compiling..."
g++ -std=c++17 -O2 -Wall mapper.cpp -o mapper || exit 1
g++ -std=c++17 -O2 -Wall combiner.cpp -o combiner || exit 1
g++ -std=c++17 -O2 -Wall reducer.cpp -o reducer || exit 1
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o sequential || exit 1

# small helper: seconds between two "date +%s%N" values (nanoseconds)
seconds() {
    echo "scale=3; ($2 - $1) / 1000000000" | bc
}

TOTAL_START=$(date +%s%N)

# split the input into P chunks: chunk_00, chunk_01, ...
# l/P means split into P pieces without cutting any line in the middle
# (only chunk_00 gets the "N K S" header line, which is fine, the mapper handles that)
STAGE_START=$(date +%s%N)
split -d -a 2 -n l/$P "$INPUT_FILE" "$WORK/chunk_"
STAGE_END=$(date +%s%N)
SPLIT_TIME=$(seconds $STAGE_START $STAGE_END)

# stage 1: mapper (P tasks in parallel, task i works on chunk i)
STAGE_START=$(date +%s%N)
srun --ntasks=$P bash -c '
    TID=$(printf "%02d" $SLURM_PROCID)
    ./mapper < "$WORK/chunk_$TID" > "$WORK/map_$TID.out"
'
STAGE_END=$(date +%s%N)
MAP_TIME=$(seconds $STAGE_START $STAGE_END)

# stage 2: local sort of every mapper's output (P tasks in parallel)
# LC_ALL=C: compare plain bytes, so equal keys are always next to each other
STAGE_START=$(date +%s%N)
srun --ntasks=$P bash -c '
    TID=$(printf "%02d" $SLURM_PROCID)
    LC_ALL=C sort "$WORK/map_$TID.out" > "$WORK/sort_$TID.out"
'
STAGE_END=$(date +%s%N)
SORT1_TIME=$(seconds $STAGE_START $STAGE_END)

# stage 3: combiner (P tasks in parallel)
STAGE_START=$(date +%s%N)
srun --ntasks=$P bash -c '
    TID=$(printf "%02d" $SLURM_PROCID)
    ./combiner < "$WORK/sort_$TID.out" > "$WORK/comb_$TID.out"
'
STAGE_END=$(date +%s%N)
COMBINE_TIME=$(seconds $STAGE_START $STAGE_END)

# stage 4: shuffle = gather all combiner outputs on the master and sort them together
# every comb file is already sorted, so "sort -m" only merges them (much faster than sorting again)
STAGE_START=$(date +%s%N)
LC_ALL=C sort -m "$WORK"/comb_*.out > "$WORK/shuffled.out"
STAGE_END=$(date +%s%N)
SHUFFLE_TIME=$(seconds $STAGE_START $STAGE_END)

# stage 5: one reducer on the master
STAGE_START=$(date +%s%N)
./reducer < "$WORK/shuffled.out" > "$OUTPUT_FILE"
STAGE_END=$(date +%s%N)
REDUCE_TIME=$(seconds $STAGE_START $STAGE_END)

TOTAL_END=$(date +%s%N)
TOTAL_TIME=$(seconds $TOTAL_START $TOTAL_END)

# how much data moves between the stages (useful for the report)
MAP_LINES=$(cat "$WORK"/map_*.out | wc -l)
COMB_LINES=$(cat "$WORK"/comb_*.out | wc -l)

echo ""
echo "Split:           ${SPLIT_TIME}s"
echo "Map:             ${MAP_TIME}s"
echo "Sort (local):    ${SORT1_TIME}s"
echo "Combine:         ${COMBINE_TIME}s"
echo "Shuffle (merge): ${SHUFFLE_TIME}s"
echo "Reduce:          ${REDUCE_TIME}s"
echo "-------------------------"
echo "TOTAL:           ${TOTAL_TIME}s"
echo ""
echo "Lines after map:     $MAP_LINES"
echo "Lines after combine: $COMB_LINES"
echo ""
echo "Output written to $OUTPUT_FILE"

# correctness check: compare with hw2's sequential program
./sequential < "$INPUT_FILE" > "$WORK/seq.out" 2> /dev/null
# (compare_outputs.py allows tiny floating point rounding differences in the last digits of big
# sums, which come from adding the numbers in a different order, everything else must be exact)
RESULT=$(python3 compare_outputs.py "$WORK/seq.out" "$OUTPUT_FILE")
if [ "$RESULT" = "FAIL" ]; then
    echo "CORRECTNESS: FAILED (different from sequential)"
    python3 compare_outputs.py "$WORK/seq.out" "$OUTPUT_FILE" > /dev/null
else
    echo "CORRECTNESS: PASSED ($RESULT, compared with sequential)"
fi

# delete the temporary files
rm -rf "$WORK"
