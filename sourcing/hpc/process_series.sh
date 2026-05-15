#!/bin/bash
#
# sbatch script for DICOM study-level processing on BCH HPC.
#
# Stage 1 (this job): organizeinputs.py — triage, dcm2niix, layouts.
# Stage 2 (dispatched at the end): nnunet/predict.sh — GPU inference.
#
# Marker contract:
#   .organize_done    organizeinputs.py succeeded; nnUNet sbatch was dispatched
#   .organize_failed  organizeinputs.py failed; no nnUNet runs
#   .nnunet_done      nnUNet inference succeeded
#   .nnunet_failed    nnUNet inference failed
#
# The submitter waits for ALL relevant markers before pulling back. See
# submitter.py for the policy (organize_failed = pull back early; both done =
# pull back as success; organize_done + nnunet_failed = partial; etc).
#
# Invoked as:
#   sbatch --job-name=dicom-<study_key> process_series.sh <hpc_scratch_dir>

#SBATCH --job-name=dicom-process
#SBATCH --partition=bch-compute
#SBATCH --time=4:00:00
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
echo "DICOM study processing job (stage 1: organizeinputs)"
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
    touch "$scratch/.organize_failed"
    exit 2
fi

# --- python environment ----------------------------------------------------

CONDA_ENV=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/fcd4pacs/MAP/fcd_env

if [ ! -x "$CONDA_ENV/bin/python3" ]; then
    echo "!! No python3 at $CONDA_ENV/bin/python3"
    touch "$scratch/.organize_failed"
    exit 3
fi

export PATH="$CONDA_ENV/bin:$PATH"
echo "Python: $(which python3) ($(python3 --version 2>&1))"

# --- organizeinputs (pick T1w + FLAIR, convert, arrange) -------------------

REPO_ROOT=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
ORGANIZER="$REPO_ROOT/organizeinputs/organizeinputs.py"
RULES="$REPO_ROOT/organizeinputs/rules.json"

DCM2NIIX_SIF="${DCM2NIIX_SIF:-$REPO_ROOT/containers/dcm2niix.sif}"
SINGULARITY_BIN="${SINGULARITY_BIN:-singularity}"

if [ ! -f "$ORGANIZER" ]; then
    echo "!! organizeinputs.py not found: $ORGANIZER"
    touch "$scratch/.organize_failed"
    exit 4
fi
if [ ! -f "$RULES" ]; then
    echo "!! rules.json not found: $RULES"
    touch "$scratch/.organize_failed"
    exit 5
fi
if [ ! -f "$DCM2NIIX_SIF" ]; then
    echo "!! dcm2niix.sif not found: $DCM2NIIX_SIF"
    touch "$scratch/.organize_failed"
    exit 6
fi
if ! command -v "$SINGULARITY_BIN" >/dev/null 2>&1; then
    if command -v module >/dev/null 2>&1; then
        module load singularity 2>/dev/null || true
    fi
fi
if ! command -v "$SINGULARITY_BIN" >/dev/null 2>&1; then
    echo "!! singularity not on PATH"
    touch "$scratch/.organize_failed"
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

echo "organizeinputs.py finished: $(date -u +%Y-%m-%dT%H-%M-%SZ), rc=$rc"

if [ $rc -ne 0 ]; then
    touch "$scratch/.organize_failed"
    echo "Touched $scratch/.organize_failed (see $output_dir/triage_report.json)"
    exit $rc
fi

touch "$scratch/.organize_done"
echo "Touched $scratch/.organize_done"

# --- dispatch GPU sbatch jobs ---------------------------------------------
# Currently meld_graph only. nnunet is DISABLED until T1/FLAIR registration
# is added to organizeinputs.py (without it, the trained model fails on
# unregistered inputs because T1 and FLAIR have different shapes).
# To re-enable nnunet later: uncomment the block below + restore the
# nnunet markers in storagescp/submitter.py:_classify_completion().

study_key=$(basename "$scratch")

# Helper: dispatch a downstream GPU sbatch. Args:
#   $1 = friendly name (for log)
#   $2 = predict.sh path
#   $3 = container .sif path
#   $4 = additional env to pass through to sbatch (e.g. "FOO=bar,BAZ=qux")
#   $5 = marker stem to write on dispatch-failure (.X_failed)
#
# Behavior:
#   - If predict.sh or .sif is missing, writes the failure marker locally and
#     prints a helpful message. Does NOT abort — process_series.sh exits 0 so
#     submitter sees .organize_done and pulls back what's there.
#   - On success, prints the new sbatch jobid for log correlation.
dispatch_downstream() {
    local name="$1"
    local script="$2"
    local sif="$3"
    local extra_env="$4"
    local marker_stem="$5"

    echo
    echo "------------------------------------------------------------"
    echo "Dispatching $name (GPU sbatch)..."
    echo "  predict.sh: $script"
    echo "  sif:        $sif"
    echo "  job name:   $name-$study_key"

    if [ ! -f "$script" ]; then
        echo "!! $script not found"
        echo "   Marking $name step as failed; submitter will pull back as partial."
        touch "$scratch/.${marker_stem}_failed"
        return 1
    fi
    if [ ! -f "$sif" ]; then
        echo "!! $sif not found"
        echo "   Marking $name step as failed."
        touch "$scratch/.${marker_stem}_failed"
        return 2
    fi

    local sbatch_out
    sbatch_out=$(sbatch \
        --job-name="$name-$study_key" \
        --export="ALL,$extra_env,SINGULARITY_BIN=$SINGULARITY_BIN" \
        "$script" \
        "$scratch" 2>&1)
    local sb_rc=$?

    echo "$sbatch_out"

    if [ $sb_rc -ne 0 ]; then
        echo "!! sbatch dispatch failed (rc=$sb_rc)"
        touch "$scratch/.${marker_stem}_failed"
        return 3
    fi

    local jobid
    jobid=$(echo "$sbatch_out" | grep -oP 'Submitted batch job \K\d+' | head -1)
    echo "$name sbatch jobid: ${jobid:-?}"
    echo "  watch with: squeue -j ${jobid:-?}  or  sacct -j ${jobid:-?}"
    return 0
}

# # nnUNet — DISABLED (T1/FLAIR shape mismatch breaks inference)
# # To re-enable: uncomment these 4 lines AND restore nnunet markers in submitter.py
# NNUNET_PREDICT="$REPO_ROOT/nnunet/predict.sh"
# NNUNET_SIF="${NNUNET_SIF:-$REPO_ROOT/containers/nnunet.sif}"
# dispatch_downstream "nnunet" "$NNUNET_PREDICT" "$NNUNET_SIF" \
#     "NNUNET_SIF=$NNUNET_SIF" \
#     "nnunet"

# meld_graph (GPU image)
MELD_PREDICT="$REPO_ROOT/meld_graph/predict.sh"
MELD_SIF="${MELD_SIF:-$REPO_ROOT/containers/meld_graph_gpu.sif}"
dispatch_downstream "meld-graph" "$MELD_PREDICT" "$MELD_SIF" \
    "MELD_SIF=$MELD_SIF" \
    "meld_graph"

echo
echo "Stage 1 finished. Downstream GPU jobs (if dispatched) running asynchronously."
exit 0
