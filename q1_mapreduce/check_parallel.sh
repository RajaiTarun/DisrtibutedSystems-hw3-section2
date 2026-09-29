#!/bin/bash
#SBATCH --job-name=q8-check-parallel
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=00:30:00
#SBATCH --output=parallel_%j.log
#SBATCH --error=parallel_%j.err

# proves that the map tasks really run in parallel (and on different nodes when we ask for more nodes)
#
# 1. runs the real map phase (mapper | sort | combiner) on P pieces with srun, like bench_slurm.sh
#    every task writes down: which node it ran on, which cpu cores it may use, its start and end time
# 2. runs the SAME P pieces again, one after another, in a single process (the "sequential" way)
# 3. prints a table per task and compares the two
#
# if the tasks run in parallel:
#   - their start..end times overlap
#   - the sum of the task times is much bigger than the wall-clock time of the map phase
#   - the parallel map phase is much faster than running the pieces one after another
# if we ask for several nodes, the "node" column shows different hostnames
#
# usage (from inside q1_mapreduce):
#   sbatch check_parallel.sh                      -> 8 tasks on 1 node
#   sbatch --nodes=4 check_parallel.sh            -> 8 tasks spread over 4 nodes
#   CHECK_N=1000000 sbatch check_parallel.sh      -> smaller input (default 10M records)

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    cd "$SLURM_SUBMIT_DIR"
fi

N=${CHECK_N:-10000000}
P=${SLURM_NTASKS:-8}

WORK=$(pwd)/check_work_${SLURM_JOB_ID:-local}
rm -rf "$WORK"
mkdir -p "$WORK"
export WORK

echo "========================================="
echo "Job id:        $SLURM_JOB_ID"
echo "Allocated:     $SLURM_JOB_NODELIST ($SLURM_JOB_NUM_NODES nodes)"
echo "Tasks (P):     $P"
echo "Input size N:  $N"
echo "========================================="

g++ -std=c++17 -O2 mapper.cpp -o mapper || exit 1
g++ -std=c++17 -O2 combiner.cpp -o combiner || exit 1

python3 ../q8/generate_dataset.py --n "$N" --k 10 --s 100 --seed 42 --out "$WORK/data.txt" > /dev/null
split -d -a 2 -n l/$P "$WORK/data.txt" "$WORK/chunk_"

seconds() {
    echo "scale=2; ($2 - $1) / 1000000000" | bc
}

# ---------------- 1. parallel: one srun task per piece ----------------
T0=$(date +%s%N)
export T0
srun --ntasks=$P bash -c '
    TID=$(printf "%02d" $SLURM_PROCID)
    START=$(date +%s%N)
    ./mapper < "$WORK/chunk_$TID" | LC_ALL=C sort | ./combiner > "$WORK/comb_$TID.out"
    END=$(date +%s%N)
    CPUS=$(grep Cpus_allowed_list /proc/self/status 2> /dev/null | cut -f2)
    echo "$TID $(hostname) ${CPUS:--} $START $END" > "$WORK/task_$TID.txt"
'
T1=$(date +%s%N)
PARALLEL=$(seconds $T0 $T1)

echo ""
echo "--- parallel map phase: one srun task per piece ---"
printf "%-5s %-12s %-10s %10s %10s %10s\n" task node cpus start_s end_s time_s
SUM=0
for f in "$WORK"/task_*.txt; do
    read TID HOST CPUS START END < "$f"
    printf "%-5s %-12s %-10s %10s %10s %10s\n" "$TID" "$HOST" "$CPUS" \
        "$(seconds $T0 $START)" "$(seconds $T0 $END)" "$(seconds $START $END)"
    SUM=$(echo "scale=2; $SUM + ($END - $START) / 1000000000" | bc)
done
HOSTS=$(cut -d' ' -f2 "$WORK"/task_*.txt | sort -u | tr '\n' ' ')
NUM_HOSTS=$(cut -d' ' -f2 "$WORK"/task_*.txt | sort -u | wc -l)

# ---------------- 2. sequential: the same pieces one after another ----------------
T2=$(date +%s%N)
for chunk in "$WORK"/chunk_??; do
    ./mapper < "$chunk" | LC_ALL=C sort | ./combiner > "$chunk.seq.out"
done
T3=$(date +%s%N)
SEQUENTIAL=$(seconds $T2 $T3)

# the outputs must be the same both ways (same program, same pieces)
SAME=yes
for chunk in "$WORK"/chunk_??; do
    TID=${chunk##*_}
    cmp -s "$chunk.seq.out" "$WORK/comb_$TID.out" || SAME=no
done

echo ""
echo "--- summary ---"
echo "Nodes the tasks actually ran on:        $NUM_HOSTS ($HOSTS)"
echo "Wall-clock time of the parallel phase:  ${PARALLEL}s"
echo "Sum of the $P task times:                ${SUM}s"
echo "Average tasks running at the same time: $(echo "scale=1; $SUM / $PARALLEL" | bc)"
echo "Same $P pieces run one after another:    ${SEQUENTIAL}s"
echo "Parallel speedup over one-after-another: $(echo "scale=2; $SEQUENTIAL / $PARALLEL" | bc)x"
echo "Outputs identical both ways:             $SAME"

rm -rf "$WORK"
