#!/bin/bash
#SBATCH --job-name=q8-mr-extra
#SBATCH --ntasks=9
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=01:00:00
#SBATCH --output=extra_%j.log
#SBATCH --error=extra_%j.err

# extra measurements for q1, on top of bench_slurm.sh
#
#   PART=memory     peak memory (max RSS) of every process, N = 50M, 4 and 8 workers:
#                   sequential, every mpi rank, and every mapreduce mapper / sort / combiner / reducer
#                   -> results/extra_memory.csv
#
#   PART=stages     N = 10M, 4 mappers, with mapper, sort and combiner run as SEPARATE stages
#                   (in bench_slurm.sh they are one pipe), so we can see which one takes the time
#                   -> results/extra_stages.csv
#
#   PART=multinode  N = 50M, mapreduce with one mapper per slurm task, spread over several nodes
#                   -> results/extra_multinode.csv
#
# time and memory of each program are measured with:  /usr/bin/time -f "%e %M"
#   %e = elapsed seconds, %M = max resident memory (RSS) in KB
#
# usage (from inside q1_mapreduce):
#   sbatch bench_extra.sh                                             -> memory + stages (1 node)
#   PART=multinode sbatch --nodes=4 --ntasks=8 bench_extra.sh         -> 8 mappers on 4 nodes
# run them one after the other (not at the same time), both use a few GB of disk

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    cd "$SLURM_SUBMIT_DIR"
fi

PART=${PART:-"memory stages"}

# input sizes of the parts (EXTRA_N changes all of them, only meant for a quick test run)
N_MEMORY=${EXTRA_N:-50000000}
N_STAGES=${EXTRA_N:-10000000}
N_MULTINODE=${EXTRA_N:-50000000}
K=10
S=100
SEED=42

# the program that measures time + memory (can be changed only for testing on a mac)
TIME_CMD=${TIME_CMD:-/usr/bin/time}
if [ ! -x "$TIME_CMD" ]; then
    echo "ERROR: $TIME_CMD not found, cannot measure memory"
    exit 1
fi
export TIME_CMD

module load openmpi/4.1.5 2> /dev/null

WORK=$(pwd)/extra_work_${SLURM_JOB_ID:-local}
rm -rf "$WORK"
mkdir -p "$WORK" results
export WORK

echo "========================================="
echo "Job id:  $SLURM_JOB_ID"
echo "Nodes:   $SLURM_JOB_NODELIST ($SLURM_JOB_NUM_NODES nodes)"
echo "Tasks:   $SLURM_NTASKS"
echo "Parts:   $PART"
echo "========================================="

echo "Compiling..."
g++ -std=c++17 -O2 -Wall mapper.cpp -o mapper || exit 1
g++ -std=c++17 -O2 -Wall combiner.cpp -o combiner || exit 1
g++ -std=c++17 -O2 -Wall reducer.cpp -o reducer || exit 1
g++ -std=c++17 -O2 ../q8/sequential.cpp ../q8/q8_common.cpp -o sequential || exit 1
if [[ "$PART" == *memory* ]]; then
    mpicxx -std=c++17 -O2 ../q8/mpi.cpp ../q8/q8_common.cpp -o mpi_q8 || exit 1
fi

seconds() {
    echo "scale=3; ($2 - $1) / 1000000000" | bc
}

# reads a file written by /usr/bin/time -f "%e %M" and prints "elapsed_s,max_rss_mb"
read_measurement() {
    # the numbers are on the last line (the line before can be a warning from time)
    tail -1 "$1" | awk '{ printf "%s,%.1f", $1, $2 / 1024 }'
}

make_dataset() {
    python3 ../q8/generate_dataset.py --n "$1" --k "$K" --s "$S" --seed "$SEED" --out "$WORK/data.txt"
}

split_input() {
    rm -f "$WORK"/chunk_*
    if [ "$1" -eq 1 ]; then
        cp "$WORK/data.txt" "$WORK/chunk_00"
    else
        split -d -a 2 -n l/$1 "$WORK/data.txt" "$WORK/chunk_"
    fi
}

check() {
    python3 compare_outputs.py "$WORK/seq.out" "$1" 2> /dev/null
}

# ======================= PART: memory =======================
if [[ "$PART" == *memory* ]]; then
    N=$N_MEMORY
    CSV=results/extra_memory.csv
    echo "impl,N,workers,process,elapsed_s,max_rss_mb" > "$CSV"
    echo ""
    echo "=== memory, N=$N ==="
    make_dataset $N

    # sequential
    $TIME_CMD -f "%e %M" -o "$WORK/m_seq" ./sequential < "$WORK/data.txt" > "$WORK/seq.out" 2> /dev/null
    echo "sequential,$N,1,sequential,$(read_measurement "$WORK/m_seq")" >> "$CSV"
    echo "  sequential: $(read_measurement "$WORK/m_seq") (elapsed_s,max_rss_mb)"

    for W in 4 8; do
        # mpi: every rank runs under its own /usr/bin/time, rank 0 is the master
        rm -f "$WORK"/m_mpi_*
        T0=$(date +%s%N)
        mpirun -np $((W + 1)) --mca pml ob1 --mca osc ^ucx --mca btl vader,self --mca btl_vader_single_copy_mechanism none \
            bash -c '$TIME_CMD -f "%e %M" -o "$WORK/m_mpi_$OMPI_COMM_WORLD_RANK" ./mpi_q8' \
            < "$WORK/data.txt" > "$WORK/mpi.out" 2> /dev/null
        T1=$(date +%s%N)
        for f in "$WORK"/m_mpi_*; do
            RANK=${f##*_}
            NAME=worker
            if [ "$RANK" -eq 0 ]; then NAME=master; fi
            echo "mpi,$N,$W,${NAME}_rank$RANK,$(read_measurement "$f")" >> "$CSV"
        done
        echo "  mpi workers=$W: total=$(seconds $T0 $T1)s $(check "$WORK/mpi.out")"

        # mapreduce: every mapper / sort / combiner in every task runs under its own /usr/bin/time
        rm -f "$WORK"/m_mr_* "$WORK"/comb_*.out
        T0=$(date +%s%N)
        split_input $W
        srun --ntasks=$W bash -c '
            export LC_ALL=C
            TID=$(printf "%02d" $SLURM_PROCID)
            $TIME_CMD -f "%e %M" -o "$WORK/m_mr_mapper_$TID" ./mapper < "$WORK/chunk_$TID" \
              | $TIME_CMD -f "%e %M" -o "$WORK/m_mr_sort_$TID" sort \
              | $TIME_CMD -f "%e %M" -o "$WORK/m_mr_combiner_$TID" ./combiner > "$WORK/comb_$TID.out"
        '
        LC_ALL=C $TIME_CMD -f "%e %M" -o "$WORK/m_mr_shuffle" sort -m "$WORK"/comb_*.out > "$WORK/shuffled.out"
        $TIME_CMD -f "%e %M" -o "$WORK/m_mr_reducer" ./reducer < "$WORK/shuffled.out" > "$WORK/mr.out"
        T1=$(date +%s%N)
        for f in "$WORK"/m_mr_*; do
            NAME=${f#"$WORK"/m_mr_}
            echo "mapreduce,$N,$W,$NAME,$(read_measurement "$f")" >> "$CSV"
        done
        echo "  mapreduce P=$W: total=$(seconds $T0 $T1)s $(check "$WORK/mr.out")"
        rm -f "$WORK"/chunk_* "$WORK"/comb_*.out "$WORK/shuffled.out"
    done
    rm -f "$WORK/data.txt"
fi

# ======================= PART: stages =======================
if [[ "$PART" == *stages* ]]; then
    N=$N_STAGES
    P=4
    CSV=results/extra_stages.csv
    echo "stage,N,mappers,task,elapsed_s,max_rss_mb" > "$CSV"
    echo ""
    echo "=== separate stages, N=$N, P=$P ==="
    make_dataset $N
    ./sequential < "$WORK/data.txt" > "$WORK/seq.out" 2> /dev/null

    T0=$(date +%s%N)
    split_input $P
    T1=$(date +%s%N)
    echo "split,$N,$P,all,$(seconds $T0 $T1)," >> "$CSV"

    # one srun per stage, each task measures its own program; the srun wall time is the stage time
    T0=$(date +%s%N)
    srun --ntasks=$P bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        $TIME_CMD -f "%e %M" -o "$WORK/s_mapper_$TID" ./mapper < "$WORK/chunk_$TID" > "$WORK/map_$TID.out"
    '
    T1=$(date +%s%N)
    echo "mapper,$N,$P,all,$(seconds $T0 $T1)," >> "$CSV"
    MAP_BYTES=$(cat "$WORK"/map_*.out | wc -c)
    rm -f "$WORK"/chunk_*

    T0=$(date +%s%N)
    srun --ntasks=$P bash -c '
        export LC_ALL=C
        TID=$(printf "%02d" $SLURM_PROCID)
        $TIME_CMD -f "%e %M" -o "$WORK/s_sort_$TID" sort "$WORK/map_$TID.out" > "$WORK/sort_$TID.out"
    '
    T1=$(date +%s%N)
    echo "sort,$N,$P,all,$(seconds $T0 $T1)," >> "$CSV"
    rm -f "$WORK"/map_*.out

    T0=$(date +%s%N)
    srun --ntasks=$P bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        $TIME_CMD -f "%e %M" -o "$WORK/s_combiner_$TID" ./combiner < "$WORK/sort_$TID.out" > "$WORK/comb_$TID.out"
    '
    T1=$(date +%s%N)
    echo "combiner,$N,$P,all,$(seconds $T0 $T1)," >> "$CSV"
    rm -f "$WORK"/sort_*.out

    LC_ALL=C $TIME_CMD -f "%e %M" -o "$WORK/s_shuffle_00" sort -m "$WORK"/comb_*.out > "$WORK/shuffled.out"
    $TIME_CMD -f "%e %M" -o "$WORK/s_reducer_00" ./reducer < "$WORK/shuffled.out" > "$WORK/mr.out"

    for f in "$WORK"/s_*; do
        NAME=${f#"$WORK"/s_}      # eg mapper_02
        STAGE=${NAME%_*}
        TASK=${NAME##*_}
        echo "$STAGE,$N,$P,task$TASK,$(read_measurement "$f")" >> "$CSV"
    done
    echo "  mapper output: $MAP_BYTES bytes"
    echo "  result: $(check "$WORK/mr.out")"
    echo "$MAP_BYTES" > results/extra_stages_mapper_bytes.txt
    rm -f "$WORK"/comb_*.out "$WORK/shuffled.out" "$WORK/data.txt"
fi

# ======================= PART: multinode =======================
if [[ "$PART" == *multinode* ]]; then
    N=$N_MULTINODE
    P=${SLURM_NTASKS:-8}
    NODES=${SLURM_JOB_NUM_NODES:-1}
    CSV=results/extra_multinode.csv
    echo "impl,N,mappers,nodes,total_s,split_s,map_s,shuffle_s,reduce_s,correct" > "$CSV"
    echo ""
    echo "=== multinode, N=$N, P=$P, nodes=$NODES ==="
    make_dataset $N
    ./sequential < "$WORK/data.txt" > "$WORK/seq.out" 2> /dev/null

    T0=$(date +%s%N)
    split_input $P
    T1=$(date +%s%N)
    srun --ntasks=$P bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        ./mapper < "$WORK/chunk_$TID" | LC_ALL=C sort | ./combiner > "$WORK/comb_$TID.out"
    '
    T2=$(date +%s%N)
    LC_ALL=C sort -m "$WORK"/comb_*.out > "$WORK/shuffled.out"
    T3=$(date +%s%N)
    ./reducer < "$WORK/shuffled.out" > "$WORK/mr.out"
    T4=$(date +%s%N)

    RESULT=$(check "$WORK/mr.out")
    echo "mapreduce,$N,$P,$NODES,$(seconds $T0 $T4),$(seconds $T0 $T1),$(seconds $T1 $T2),$(seconds $T2 $T3),$(seconds $T3 $T4),$RESULT" >> "$CSV"
    echo "  mapreduce P=$P nodes=$NODES: total=$(seconds $T0 $T4)s map=$(seconds $T1 $T2)s $RESULT"
    rm -f "$WORK"/chunk_* "$WORK"/comb_*.out "$WORK/shuffled.out" "$WORK/data.txt"
fi

rm -rf "$WORK"

echo ""
echo "========================================="
echo "Done. Results:"
for f in results/extra_*.csv; do
    echo "--- $f"
    sed -e 's/,,/,-,/g' -e 's/,$/,-/' "$f" | column -t -s','
done
echo "========================================="
