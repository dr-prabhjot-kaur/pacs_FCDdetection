#!/bin/bash
#
# GPU sbatch wrapper for meld_graph prediction.
# Dispatched by process_series.sh as a separate sbatch job after
# organizeinputs.py succeeds.
#
# Deployed to:
#   HPC:  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh
#
# Usage:
#   sbatch meld_graph/predict.sh <hpc_scratch_dir>
#
# Inputs (organizeinputs.py wrote these into scratch):
#   <scratch>/output/meld/input/MELD_H52_3T_FCD_<subj>/T1/T1.nii.gz
#                                                     /FLAIR/FLAIR.nii.gz
#
# Outputs (meld_graph writes here):
#   <scratch>/output/meld/output/predictions_reports/MELD_H52_3T_FCD_<subj>/
#
# Markers (consumed by submitter.py):
#   .meld_graph_done    on success
#   .meld_graph_failed  on failure

#SBATCH --job-name=meld-graph-predict
#SBATCH --partition=bch-gpu
#SBATCH --gres=gpu:1
#SBATCH --time=24:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH --output=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/meld_graph_%j.out
#SBATCH --error=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/meld_graph_%j.err

set -u

scratch=$1

# --- Paths --------------------------------------------------------------

REPO_ROOT=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
SIF="${MELD_SIF:-$REPO_ROOT/containers/meld_graph_gpu.sif}"

MELD_DATA_CENTRAL="${MELD_DATA_CENTRAL:-/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/MELDgraph2026/meld_data}"

FS_LICENSE="${FS_LICENSE:-/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/MELDgraph2026/meld_graph/license.txt}"
MELD_LICENSE="${MELD_LICENSE:-/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/MELDgraph2026/meld_graph/meld_license.txt}"

SINGULARITY_BIN="${SINGULARITY_BIN:-singularity}"
SITE_CODE="${SITE_CODE:-H52}"

# Apptainer cache redirects (avoid home quota disaster on long jobs).
# These are HOST-side env vars, read by singularity itself before the
# container starts. They get stripped at container boundary by --cleanenv
# (see the singularity exec call below).
export TMPDIR=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/tmp
export APPTAINER_CACHEDIR=$TMPDIR/apptainer_cache
export APPTAINER_TMPDIR=$TMPDIR/apptainer_tmp
export SINGULARITY_CACHEDIR=$APPTAINER_CACHEDIR
export SINGULARITY_TMPDIR=$APPTAINER_TMPDIR
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR"

# Per-study paths
study_meld_dir="$scratch/output/meld"             # becomes /data inside container
input_dir="$study_meld_dir/input"
output_dir="$study_meld_dir/output"
log_file="$scratch/meld_graph_job.log"

mkdir -p "$output_dir"

exec > >(tee -a "$log_file") 2>&1

echo "========================================================"
echo "meld_graph inference job"
echo "  Scratch:   $scratch"
echo "  Input:     $input_dir"
echo "  Output:    $output_dir"
echo "  SIF:       $SIF"
echo "  Central meld_data: $MELD_DATA_CENTRAL"
echo "  Slurm job: ${SLURM_JOB_ID:-N/A}"
echo "  Job name:  ${SLURM_JOB_NAME:-N/A}"
echo "  Node:      $(hostname)"
echo "  GPU:       ${CUDA_VISIBLE_DEVICES:-?}"
echo "  Started:   $(date -u +%Y-%m-%dT%H-%M-%SZ)"
echo "  Site code: $SITE_CODE"
echo "========================================================"

# --- Pre-flight ----------------------------------------------------------

if [ ! -f "$SIF" ]; then
    echo "!! sif not found: $SIF"
    touch "$scratch/.meld_graph_failed"
    exit 2
fi

if ! command -v "$SINGULARITY_BIN" >/dev/null 2>&1; then
    if command -v module >/dev/null 2>&1; then
        module load singularity 2>/dev/null || true
    fi
fi
if ! command -v "$SINGULARITY_BIN" >/dev/null 2>&1; then
    echo "!! singularity not on PATH"
    touch "$scratch/.meld_graph_failed"
    exit 3
fi

if [ ! -d "$input_dir" ]; then
    echo "!! input dir does not exist: $input_dir"
    touch "$scratch/.meld_graph_failed"
    exit 4
fi

# Should be exactly one MELD_<SITE>_3T_FCD_<subj> dir for the production pipeline
subjects=( $(ls -1 "$input_dir" 2>/dev/null | grep "^MELD_${SITE_CODE}_") )
if [ ${#subjects[@]} -eq 0 ]; then
    echo "!! No MELD_${SITE_CODE}_* subject dir in $input_dir"
    ls "$input_dir" 2>/dev/null
    touch "$scratch/.meld_graph_failed"
    exit 5
fi
echo "Subjects found in input/:"
printf '    %s\n' "${subjects[@]}"

# Verify central harmonisation outputs exist
echo
echo "Verifying central harmonisation outputs for site $SITE_CODE..."
harmo_ref_dir="$MELD_DATA_CENTRAL/output/preprocessed_surf_data"
site_harmo_check="$harmo_ref_dir/MELD_${SITE_CODE}"
if [ ! -d "$site_harmo_check" ]; then
    echo "!! Site harmonisation dir missing: $site_harmo_check"
    echo "!! Did you run the harmonisation sbatch successfully?"
    touch "$scratch/.meld_graph_failed"
    exit 6
fi
echo "  Site harmonisation files:"
ls -lh "$site_harmo_check/" 2>/dev/null | sed 's/^/    /'

# --- Compose /data tree -------------------------------------------------

echo
echo "Composing per-study meld_data tree..."
mkdir -p "$output_dir/preprocessed_surf_data"

site_harmo_dir="$harmo_ref_dir/MELD_${SITE_CODE}"
cp -rp "$site_harmo_dir" "$output_dir/preprocessed_surf_data/MELD_${SITE_CODE}"
echo "Copied site harmonisation params:"
ls -lh "$output_dir/preprocessed_surf_data/MELD_${SITE_CODE}/" | sed 's/^/    /'

echo
echo "Per-study /data tree state:"
echo "  input/ subjects:              $(ls -1 "$input_dir" 2>/dev/null | wc -l)"
echo "  output/ harmo references:     $(ls -d "$output_dir"/preprocessed_surf_data/*/ 2>/dev/null | wc -l)"

# --- Discover prediction script inside container ------------------------

CANDIDATES=(
    "/app/scripts/new_patient_pipeline/new_pt_pipeline.py"
    "/app/scripts/new_site/new_site_harmonisation.py"
    "/app/new_pt_pipeline.py"
)

PRED_SCRIPT=""
for c in "${CANDIDATES[@]}"; do
    if "$SINGULARITY_BIN" exec "$SIF" test -f "$c" 2>/dev/null; then
        PRED_SCRIPT="$c"
        break
    fi
done

if [ -z "$PRED_SCRIPT" ]; then
    echo "!! Could not locate prediction script in $SIF"
    "$SINGULARITY_BIN" exec "$SIF" find /app -maxdepth 4 -name "new_*.py" 2>/dev/null
    touch "$scratch/.meld_graph_failed"
    exit 7
fi
echo "Using: $PRED_SCRIPT"

# --- Set up bind mounts + licenses --------------------------------------
# meld_graph requires:
#   1. FreeSurfer license at /data/license.txt (file inside MELD_DATA_PATH)
#   2. MELD_LICENSE env var pointing to the meld license FILE PATH inside
#      the container (the code does os.path.exists() on this var, then
#      opens it for reading). NOT the file content.

cp -f "$FS_LICENSE" "$study_meld_dir/license.txt"
cp -f "$MELD_LICENSE" "$study_meld_dir/meld_license.txt"
chmod 0644 "$study_meld_dir/license.txt" "$study_meld_dir/meld_license.txt"

# MELD_LICENSE inside container = path under /data
export APPTAINERENV_MELD_LICENSE=/data/meld_license.txt
export SINGULARITYENV_MELD_LICENSE=/data/meld_license.txt

export APPTAINER_BINDPATH="$study_meld_dir:/data"
export SINGULARITY_BINDPATH="$APPTAINER_BINDPATH"

echo
echo "Bind path:"
echo "  $APPTAINER_BINDPATH" | tr ',' '\n' | sed 's/^/    /'
echo
echo "Licenses placed in /data:"
ls -la "$study_meld_dir/license.txt" "$study_meld_dir/meld_license.txt" | sed 's/^/    /'
echo "MELD_LICENSE env (path inside container): $APPTAINERENV_MELD_LICENSE"

# --- Run prediction ------------------------------------------------------

ids_file="$study_meld_dir/_subjects_for_predict.txt"
ls -1 "$input_dir" | grep "^MELD_${SITE_CODE}_" > "$ids_file"
echo
echo "Subjects to predict on:"
cat "$ids_file" | sed 's/^/    /'

echo
echo "============================================================"
echo "Running meld_graph prediction..."
echo "  Started: $(date -u +%Y-%m-%dT%H-%M-%SZ)"
echo "============================================================"

# IMPORTANT: --cleanenv prevents host TMPDIR / PATH / etc. from leaking
# into the container. The host TMPDIR points at /lab-share/... which is
# NOT bind-mounted into the container, so FreeSurfer's mktemp inside the
# container fails when it sees that path. Without --cleanenv, FastSurfer's
# recon-all step dies with:
#   mktemp: failed to create file via template '/lab-share/.../tmp/tmp.XXXXXXXXXX'
#   OSError: [Errno 30] Read-only file system: '-s'
# Container-side env vars are passed via APPTAINERENV_* (set above).

"$SINGULARITY_BIN" exec --nv --cleanenv "$SIF" \
    python "$PRED_SCRIPT" \
        -ids /data/_subjects_for_predict.txt \
        -harmo_code "$SITE_CODE" \
        --fastsurfer
rc=$?

echo
echo "Finished: $(date -u +%Y-%m-%dT%H-%M-%SZ), rc=$rc"

# --- Verify outputs -----------------------------------------------------

echo
echo "Outputs produced:"
echo "  fs_outputs:           $(ls -d "$output_dir"/fs_outputs/*/ 2>/dev/null | wc -l)"
echo "  preprocessed_surf:    $(ls -d "$output_dir"/preprocessed_surf_data/*/ 2>/dev/null | wc -l)"
echo "  predictions_reports:  $(ls -d "$output_dir"/predictions_reports/*/ 2>/dev/null | wc -l)"

n_predicted=0
for s in "${subjects[@]}"; do
    if [ -d "$output_dir/predictions_reports/$s" ]; then
        n_predicted=$((n_predicted + 1))
        echo "  predicted: $s"
        ls "$output_dir/predictions_reports/$s/" 2>/dev/null | sed 's/^/      /'
    else
        echo "  NOT PREDICTED: $s"
    fi
done

# --- Cleanup harmo reference (optional) ---------------------------------
# Remove site reference from per-study output to keep archive lean.
# Set MELD_KEEP_HARMO_REF=1 to keep for debugging.

if [ "${MELD_KEEP_HARMO_REF:-0}" -ne 1 ]; then
    echo
    echo "Cleaning up site harmonisation reference from per-study output/..."
    rm -rf "$output_dir/preprocessed_surf_data/MELD_${SITE_CODE}"
fi

# --- Mark completion ----------------------------------------------------

if [ $rc -eq 0 ] && [ "$n_predicted" -ge 1 ]; then
    touch "$scratch/.meld_graph_done"
    echo "Touched $scratch/.meld_graph_done"
    exit 0
else
    touch "$scratch/.meld_graph_failed"
    echo "Touched $scratch/.meld_graph_failed (rc=$rc, predicted=$n_predicted)"
    exit ${rc:-1}
fi
