#!/bin/bash
#
# sbatch script for DICOM series processing on BCH HPC.
#
# Invoked as:
#   sbatch process_series.sh <hpc_scratch_dir>
#
# The <hpc_scratch_dir> is a /lab-share/... path containing:
#   <scratch>/input/    DICOM files rsynced from workstation
#   <scratch>/output/   where this script writes results
#
# Completion signalling:
#   On success  -> touches <scratch>/.done
#   On failure  -> touches <scratch>/.failed
# The submitter on the workstation polls for these markers.

#SBATCH --job-name=dicom-process
#SBATCH --partition=bch-compute
#SBATCH --time=12:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4
#SBATCH --output=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/%j.out
#SBATCH --error=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/%j.err

set -u

scratch=$1
input_dir="$scratch/input"
output_dir="$scratch/output"
log_file="$scratch/job.log"

mkdir -p "$output_dir"

# Redirect ALL output from here onward to BOTH slurm's log AND the scratch
# job.log, so the workstation submitter can see what happened even if
# slurm's log isn't reachable.
exec > >(tee -a "$log_file") 2>&1

echo "========================================================"
echo "DICOM processing job"
echo "  Scratch:   $scratch"
echo "  Input:     $input_dir"
echo "  Output:    $output_dir"
echo "  Slurm job: ${SLURM_JOB_ID:-N/A}"
echo "  Node:      $(hostname)"
echo "  Started:   $(date -u +%Y-%m-%dT%H-%M-%SZ)"
echo "========================================================"

# --- sanity checks ----------------------------------------------------------

if [ ! -d "$input_dir" ]; then
    echo "!! Input directory missing: $input_dir"
    touch "$scratch/.failed"
    exit 2
fi

# --- python environment -----------------------------------------------------
#
# Deliberately NOT using `conda activate` — it requires conda's shell
# integration which is unreliable under non-interactive sbatch. A conda
# env's bin/ is self-contained, so putting it on PATH is enough.

CONDA_ENV=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/fcd4pacs/MAP/fcd_env

if [ ! -x "$CONDA_ENV/bin/python3" ]; then
    echo "!! No python3 at $CONDA_ENV/bin/python3"
    touch "$scratch/.failed"
    exit 3
fi

export PATH="$CONDA_ENV/bin:$PATH"
echo "Python: $(which python3) ($(python3 --version 2>&1))"

# --- processing -------------------------------------------------------------

SCRIPTS_DIR=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts
PROCESSOR="${PROCESSOR:-python3 $SCRIPTS_DIR/process_stub.py}"

echo "Running: $PROCESSOR --in-dir $input_dir --out-dir $output_dir"
$PROCESSOR --in-dir "$input_dir" --out-dir "$output_dir"
rc=$?

echo "Finished: $(date -u +%Y-%m-%dT%H-%M-%SZ), rc=$rc"

# --- completion signal ------------------------------------------------------

if [ $rc -eq 0 ]; then
    touch "$scratch/.done"
    echo "Touched $scratch/.done"
    exit 0
else
    touch "$scratch/.failed"
    echo "Touched $scratch/.failed"
    exit $rc
fi
