#!/bin/bash
#
# sbatch script for DICOM study-level processing on BCH HPC.
#
# Invoked as:
#   sbatch --job-name=dicom-<study_key> process_series.sh <hpc_scratch_dir>
#
# The submitter sets --job-name dynamically per study so it can correlate
# Slurm job state back to the queue marker via sacct.
#
# <hpc_scratch_dir> is a /lab-share/... path containing:
#   <scratch>/input/    one subdir per series (named by SeriesInstanceUID)
#   <scratch>/output/   where this script writes results
#
# Completion signalling:
#   On success  -> touches <scratch>/.done
#   On failure  -> touches <scratch>/.failed

#SBATCH --job-name=dicom-process
#SBATCH --partition=bch-compute
#SBATCH --time=64:00:00
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

exec > >(tee -a "$log_file") 2>&1

echo "========================================================"
echo "DICOM study processing job"
echo "  Scratch:   $scratch"
echo "  Input:     $input_dir"
echo "  Output:    $output_dir"
echo "  Slurm job: ${SLURM_JOB_ID:-N/A}"
echo "  Job name:  ${SLURM_JOB_NAME:-N/A}"
echo "  Node:      $(hostname)"
echo "  Started:   $(date -u +%Y-%m-%dT%H-%M-%SZ)"
echo "  Series in study:"
ls -1 "$input_dir" 2>/dev/null | sed 's/^/    /'
echo "========================================================"

if [ ! -d "$input_dir" ]; then
    echo "!! Input directory missing: $input_dir"
    touch "$scratch/.failed"
    exit 2
fi

# --- python environment ----------------------------------------------------

CONDA_ENV=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/fcd4pacs/MAP/fcd_env

if [ ! -x "$CONDA_ENV/bin/python3" ]; then
    echo "!! No python3 at $CONDA_ENV/bin/python3"
    touch "$scratch/.failed"
    exit 3
fi

export PATH="$CONDA_ENV/bin:$PATH"
echo "Python: $(which python3) ($(python3 --version 2>&1))"

# --- organizeinputs (pick T1w + FLAIR, convert, arrange) -------------------

REPO_ROOT=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
ORGANIZER="$REPO_ROOT/organizeinputs/organizeinputs.py"
RULES="$REPO_ROOT/organizeinputs/rules.json"

# Singularity image with dcm2niix on PATH. Override via env if needed.
DCM2NIIX_SIF="${DCM2NIIX_SIF:-$REPO_ROOT/containers/dcm2niix.sif}"
SINGULARITY_BIN="${SINGULARITY_BIN:-singularity}"

if [ ! -f "$ORGANIZER" ]; then
    echo "!! organizeinputs.py not found: $ORGANIZER"
    touch "$scratch/.failed"
    exit 4
fi

if [ ! -f "$RULES" ]; then
    echo "!! rules.json not found: $RULES"
    touch "$scratch/.failed"
    exit 5
fi

if [ ! -f "$DCM2NIIX_SIF" ]; then
    echo "!! dcm2niix singularity image not found: $DCM2NIIX_SIF"
    touch "$scratch/.failed"
    exit 6
fi

if ! command -v "$SINGULARITY_BIN" >/dev/null 2>&1; then
    if command -v module >/dev/null 2>&1; then
        module load singularity 2>/dev/null || true
    fi
fi
if ! command -v "$SINGULARITY_BIN" >/dev/null 2>&1; then
    echo "!! singularity not on PATH"
    touch "$scratch/.failed"
    exit 7
fi

echo "Running organizeinputs.py"
echo "  rules:        $RULES"
echo "  dcm2niix sif: $DCM2NIIX_SIF"

python3 "$ORGANIZER" \
    --in-dir "$input_dir" \
    --out-dir "$output_dir" \
    --dcm2niix-sif "$DCM2NIIX_SIF" \
    --singularity "$SINGULARITY_BIN" \
    --rules "$RULES"
rc=$?

echo "Finished: $(date -u +%Y-%m-%dT%H-%M-%SZ), rc=$rc"

if [ $rc -eq 0 ]; then
    touch "$scratch/.done"
    echo "Touched $scratch/.done"
    exit 0
else
    touch "$scratch/.failed"
    echo "Touched $scratch/.failed (see $output_dir/triage_report.json)"
    exit $rc
fi
