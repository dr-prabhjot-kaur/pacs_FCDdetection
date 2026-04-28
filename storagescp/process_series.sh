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

# --- python environment (PATH-based) ---------------------------------------

CONDA_ENV=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/fcd4pacs/MAP/fcd_env

if [ ! -x "$CONDA_ENV/bin/python3" ]; then
    echo "!! No python3 at $CONDA_ENV/bin/python3"
    touch "$scratch/.failed"
    exit 3
fi

export PATH="$CONDA_ENV/bin:$PATH"
echo "Python: $(which python3) ($(python3 --version 2>&1))"

# --- derive study key (= subject id) from scratch path ---------------------
#
# The submitter creates scratch/<study_key>/{input,output}, so the parent of
# $scratch is named <study_key> = <MRN>_<DOS>_<studyhash>. This is what
# triage_and_convert.py uses as --subject-id (filenames in nnUNet, folder
# name in MELD).

study_key="$(basename "$scratch")"
echo "Study key (= subject id): $study_key"

# --- organize inputs (pick T1w + FLAIR, convert, arrange) -----------------

SCRIPTS_DIR=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts

# Singularity image with dcm2niix on PATH. Override at submit time via env if
# you build a different image:  DCM2NIIX_SIF=/path/other.sif sbatch ...
DCM2NIIX_SIF="${DCM2NIIX_SIF:-/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/containers/dcm2niix.sif}"
SINGULARITY_BIN="${SINGULARITY_BIN:-singularity}"

# MELD site label and nnUNet dataset id can also be overridden via env.
MELD_SITE="${MELD_SITE:-site01}"
NNUNET_DS_ID="${NNUNET_DS_ID:-501}"
NNUNET_DS_NAME="${NNUNET_DS_NAME:-FCD}"

if [ ! -f "$DCM2NIIX_SIF" ]; then
    echo "!! dcm2niix singularity image not found: $DCM2NIIX_SIF"
    touch "$scratch/.failed"
    exit 6
fi

# Make sure singularity is available.
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
echo "  dcm2niix sif: $DCM2NIIX_SIF"
echo "  MELD site:    $MELD_SITE"
echo "  nnUNet:       Dataset${NNUNET_DS_ID}_${NNUNET_DS_NAME}"

python3 "$SCRIPTS_DIR/organizeinputs.py" \
    --in-dir "$input_dir" \
    --out-dir "$output_dir" \
    --subject-id "$study_key" \
    --dcm2niix-sif "$DCM2NIIX_SIF" \
    --singularity "$SINGULARITY_BIN" \
    --site "$MELD_SITE" \
    --nnunet-dataset-id "$NNUNET_DS_ID" \
    --nnunet-dataset-name "$NNUNET_DS_NAME"
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
