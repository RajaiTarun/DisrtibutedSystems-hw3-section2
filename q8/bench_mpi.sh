#!/bin/bash
#SBATCH --job-name=q8-mpi-bench
#SBATCH --ntasks=9
#SBATCH --nodes=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=04:00:00
#SBATCH --output=%j.log
#SBATCH --error=%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=tarun.rajai@students.iiit.ac.in
#SBATCH -D /home/cs3401.49/ds-hw2/q8

module load openmpi/4.1.5

echo "========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Allocated nodes: $SLURM_NNODES"
echo "Total tasks: $SLURM_NTASKS"
echo "Node list: $SLURM_NODELIST"
echo "CPUs allocated to this job (SLURM_JOB_CPUS_PER_NODE): $SLURM_JOB_CPUS_PER_NODE"
echo "CPUs visible to this shell (nproc): $(nproc)"
echo "========================================="
echo ""

echo "Compiling MPI program..."
mpicxx -O2 -std=c++17 mpi.cpp q8_common.cpp -o mpi_q8
if [ $? -ne 0 ]; then
    echo "MPI compilation failed!"
    exit 1
fi
echo "Compilation successful."
echo ""

mkdir -p output
RESULTS=output/mpi_benchmark.log
printf "" > "$RESULTS"

# benchmark configuration - generate_dataset.py's RNG is seeded, so the same
# --n/--k/--s/--seed combination always regenerates byte-identical input.
# keep these three fixed when the sequential benchmark script is written
# later, so both scripts run on the exact same data.
K=10
S=100
SEED=42

# P is the TOTAL process count (rank 0 = master), so numWorkers = P-1 gives
# 1, 2, 4, 8 workers - doubling worker counts, same P values q2 benchmarks with
PROC_COUNTS=(2 3 5 9)

SIZES=(100 1000 10000 100000 1000000 20000000 50000000)

IN=bench_input.txt

for N in "${SIZES[@]}"; do
    echo "=== N=$N: generating input ==="
    python3 generate_dataset.py --n "$N" --k "$K" --s "$S" --seed "$SEED" --out "$IN"

    echo "N=$N K=$K S=$S seed=$SEED" >> "$RESULTS"

    for P in "${PROC_COUNTS[@]}"; do
        echo "=== N=$N, P=$P ==="
        echo "--- P=$P ---" >> "$RESULTS"
        # same UCX workaround as the other q8/q2/q6 scripts on this cluster.
        # btl vader,self with single_copy_mechanism=none keeps scatter/gather
        # on shared memory (fast) instead of forcing TCP loopback (slow) -
        # vader's default CMA single-copy path fails with "Read -1, errno=1"
        # on this cluster (ptrace_scope restrictions), but the non-CMA
        # (double-copy) vader path doesn't need CMA at all and still beats TCP.
        { time mpirun -np $P --mca pml ob1 --mca osc ^ucx --mca btl vader,self --mca btl_vader_single_copy_mechanism none ./mpi_q8 < "$IN" > /dev/null; } >> "$RESULTS" 2>&1
        echo "" >> "$RESULTS"
    done

    echo "=== N=$N: deleting input ==="
    rm -f "$IN"
    echo ""
done

echo "========================================="
echo "Benchmark complete. Results in $RESULTS"
echo "========================================="
