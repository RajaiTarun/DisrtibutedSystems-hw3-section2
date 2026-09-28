#!/bin/bash
#SBATCH --job-name=q8-mr-bench
#SBATCH --ntasks=9
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=04:00:00
#SBATCH --output=bench_%j.log
#SBATCH --error=bench_%j.err

# benchmark for q8: mapreduce vs hw2's mpi vs hw2's sequential, on the exact same datasets
#
# for every input size N:
#   1. generate the dataset (same generator + seed as hw2, so the same data as the hw2 benchmarks)
#   2. run sequential once  -> reference output + baseline time
#   3. run mpi       with 1, 2, 4, 8 workers (mpirun -np workers+1, rank 0 is the master)
#   4. run mapreduce with 1, 2, 4, 8 mappers
#   5. run mapreduce WITHOUT the combiner (only for the smaller sizes), to see how much the combiner saves
# every run is checked against the sequential output
#
# mapreduce here: the map phase of each task is  mapper < chunk | sort | combiner > comb_XX
# (piped, so the big mapper output never has to be written to disk, just like a hadoop map task
# sorts and combines its own output before the reducers get it)
# then: shuffle = sort -m of all comb files, and reduce = 1 reducer
#
# all results go to results/bench_results.csv (one line per run)
#
# usage (from inside the q1_mapreduce folder, ../q8 must also be there):
#   sbatch bench_slurm.sh

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    cd "$SLURM_SUBMIT_DIR"
fi

# ---------------- configuration ----------------
# K, S, SEED are the same as hw2's bench_mpi.sh, so it is the same data as hw2
K=10
S=100
SEED=42
SIZES=(1000000 10000000 20000000 50000000)
WORKERS=(1 2 4 8)
# the "no combiner" runs send the full mapper output to the shuffle, so only do them for the smaller sizes
NO_COMBINER_SIZES=(1000000 10000000)

# for a quick test run, the sizes can be changed without editing this file:
#   BENCH_SIZES="10000 100000" sbatch bench_slurm.sh
if [ -n "$BENCH_SIZES" ]; then
    SIZES=($BENCH_SIZES)
    NO_COMBINER_SIZES=($BENCH_SIZES)
fi
# -----------------------------------------------

module load openmpi/4.1.5

WORK=$(pwd)/bench_work_${SLURM_JOB_ID:-local}
rm -rf "$WORK"
mkdir -p "$WORK" results
export WORK

CSV=results/bench_results.csv
echo "impl,N,workers,total_s,split_s,map_s,shuffle_s,reduce_s,shuffle_lines,correct" > "$CSV"

echo "========================================="
echo "Job id:  $SLURM_JOB_ID"
echo "Nodes:   $SLURM_JOB_NODELIST"
echo "Tasks:   $SLURM_NTASKS"
echo "Sizes:   ${SIZES[*]}"
echo "Workers: ${WORKERS[*]}"
echo "========================================="

echo "Compiling..."
g++ -std=c++17 -O2 -Wall mapper.cpp -o mapper || exit 1
g++ -std=c++17 -O2 -Wall combiner.cpp -o combiner || exit 1
g++ -std=c++17 -O2 -Wall reducer.cpp -o reducer || exit 1
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o sequential || exit 1
mpicxx -std=c++17 -O2 ../q8/mpi.cpp ../q8/q8_common.cpp -o mpi_q8 || exit 1

# seconds between two "date +%s%N" values (nanoseconds)
seconds() {
    echo "scale=3; ($2 - $1) / 1000000000" | bc
}

# compares a result file with the sequential output:
# EXACT    = identical to sequential
# FP_CLOSE = only floating point rounding differences in the last digits (see compare_outputs.py)
# FAIL     = a real difference
check() {
    python3 compare_outputs.py "$WORK/seq.out" "$1" 2> /dev/null
}

# runs the mapreduce pipeline once
#   $1 = input file, $2 = number of mappers, $3 = "yes" to use the combiner, "no" to skip it
run_mapreduce() {
    local INPUT=$1
    local P=$2
    local USE_COMBINER=$3
    export USE_COMBINER

    rm -f "$WORK"/chunk_* "$WORK"/comb_*.out
    local T0=$(date +%s%N)

    # split into P chunks without cutting lines
    if [ "$P" -eq 1 ]; then
        cp "$INPUT" "$WORK/chunk_00"
    else
        split -d -a 2 -n l/$P "$INPUT" "$WORK/chunk_"
    fi
    local T1=$(date +%s%N)

    # map phase: P tasks in parallel, each does mapper | sort | combiner on its own chunk
    srun --ntasks=$P bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        if [ "$USE_COMBINER" = "yes" ]; then
            ./mapper < "$WORK/chunk_$TID" | LC_ALL=C sort | ./combiner > "$WORK/comb_$TID.out"
        else
            ./mapper < "$WORK/chunk_$TID" | LC_ALL=C sort > "$WORK/comb_$TID.out"
        fi
    '
    local T2=$(date +%s%N)

    # shuffle: merge all the (already sorted) task outputs on the master
    LC_ALL=C sort -m "$WORK"/comb_*.out > "$WORK/shuffled.out"
    local T3=$(date +%s%N)

    # reduce: 1 reducer
    ./reducer < "$WORK/shuffled.out" > "$WORK/mr.out"
    local T4=$(date +%s%N)

    local SHUFFLE_LINES=$(wc -l < "$WORK/shuffled.out")
    local IMPL=mapreduce
    if [ "$USE_COMBINER" = "no" ]; then IMPL=mapreduce_nocombiner; fi

    echo "$IMPL,$N,$P,$(seconds $T0 $T4),$(seconds $T0 $T1),$(seconds $T1 $T2),$(seconds $T2 $T3),$(seconds $T3 $T4),$SHUFFLE_LINES,$(check "$WORK/mr.out")" >> "$CSV"
    echo "  $IMPL P=$P: total=$(seconds $T0 $T4)s map=$(seconds $T1 $T2)s shuffle=$(seconds $T2 $T3)s reduce=$(seconds $T3 $T4)s lines=$SHUFFLE_LINES $(check "$WORK/mr.out")"

    rm -f "$WORK"/chunk_* "$WORK"/comb_*.out "$WORK/shuffled.out"
}

for N in "${SIZES[@]}"; do
    echo ""
    echo "=== N=$N ==="
    INPUT=$WORK/data.txt
    python3 ../q8/generate_dataset.py --n "$N" --k "$K" --s "$S" --seed "$SEED" --out "$INPUT"

    # sequential: reference output + baseline time
    T0=$(date +%s%N)
    ./sequential < "$INPUT" > "$WORK/seq.out" 2> /dev/null
    T1=$(date +%s%N)
    echo "sequential,$N,1,$(seconds $T0 $T1),,,,,,REFERENCE" >> "$CSV"
    echo "  sequential: $(seconds $T0 $T1)s"

    # mpi from hw2 (same mpirun flags as hw2's bench_mpi.sh)
    for P in "${WORKERS[@]}"; do
        T0=$(date +%s%N)
        mpirun -np $((P + 1)) --mca pml ob1 --mca osc ^ucx --mca btl vader,self --mca btl_vader_single_copy_mechanism none \
            ./mpi_q8 < "$INPUT" > "$WORK/mpi.out" 2> /dev/null
        T1=$(date +%s%N)
        echo "mpi,$N,$P,$(seconds $T0 $T1),,,,,,$(check "$WORK/mpi.out")" >> "$CSV"
        echo "  mpi workers=$P: $(seconds $T0 $T1)s $(check "$WORK/mpi.out")"
    done

    # mapreduce with the combiner
    for P in "${WORKERS[@]}"; do
        run_mapreduce "$INPUT" "$P" yes
    done

    # mapreduce without the combiner (smaller sizes only)
    for SMALL in "${NO_COMBINER_SIZES[@]}"; do
        if [ "$N" -eq "$SMALL" ]; then
            for P in "${WORKERS[@]}"; do
                run_mapreduce "$INPUT" "$P" no
            done
        fi
    done

    rm -f "$INPUT"
done

rm -rf "$WORK"

echo ""
echo "========================================="
echo "Benchmark complete. Results in $CSV"
echo "========================================="
# print the csv as a table (empty fields shown as "-" so the columns stay aligned)
sed -e 's/,,/,-,/g' -e 's/,,/,-,/g' "$CSV" | column -t -s','
