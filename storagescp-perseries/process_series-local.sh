#!/bin/bash
#
# sbatch script template for DICOM series processing.
#
# Invoked by submitter.py as:
#   sbatch process_series.sh <staging_series> <out_series> \
#          <queue_marker> <archive_root> <failed_root>
#
# On success: series is archived (renamed with timestamp), queue
#             marker renamed to .done_<ts>.
# On failure: series is moved to failed/ (preserved), queue marker
#             renamed to .failed_<ts>.
#
# No rm anywhere. Every transition is a mv so evidence survives.

#SBATCH --job-name=dicom-process
#SBATCH --time=12:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4
#SBATCH --output=/home/ch239656/work/gits/dicomtools-pacs/slurm-logs/%j.out
#SBATCH --error=/home/ch239656/work/gits/dicomtools-pacs/data_dicom/slurm-logs/%j.err

set -u  # undefined variable = error. Do NOT set -e; we handle errors explicitly.

staging_series=$1
out_series=$2
queue_marker=$3
archive_root=$4
failed_root=$5

series_name=$(basename "$staging_series")
timestamp=$(date -u +%Y-%m-%dT%H-%M-%SZ)

echo "========================================================"
echo "DICOM processing job"
echo "  Series:    $series_name"
echo "  Staging:   $staging_series"
echo "  Out:       $out_series"
echo "  Marker:    $queue_marker"
echo "  Started:   $timestamp"
echo "  Slurm job: ${SLURM_JOB_ID:-N/A}"
echo "========================================================"

# --- sanity checks ----------------------------------------------------------

if [ ! -d "$staging_series" ]; then
    echo "!! Staging directory missing: $staging_series"
    exit 2
fi

mkdir -p "$out_series"
mkdir -p "$archive_root"
mkdir -p "$failed_root"

# --- processing -------------------------------------------------------------

# Replace this with your real processor. The stub just lists files
# and writes a summary.json so you can verify the plumbing.
#
# When ready: module load python/3.x; source venv; etc.

PROCESSOR="${PROCESSOR:-python3 /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp/process_stub.py}"

$PROCESSOR --in-dir "$staging_series" --out-dir "$out_series"
rc=$?

# --- outcome ----------------------------------------------------------------

end_timestamp=$(date -u +%Y-%m-%dT%H-%M-%SZ)

if [ $rc -eq 0 ]; then
    echo "Processing succeeded (rc=0). Archiving."
    archive_target="$archive_root/${series_name}_${end_timestamp}"
    mv "$staging_series" "$archive_target"
    mv "$queue_marker" "${queue_marker%.submitted}.done_${end_timestamp}"
    echo "Archived to: $archive_target"
    exit 0
else
    echo "Processing failed (rc=$rc). Preserving inputs."
    failed_target="$failed_root/${series_name}_${end_timestamp}"
    mv "$staging_series" "$failed_target"
    mv "$queue_marker" "${queue_marker%.submitted}.failed_${end_timestamp}"
    echo "Failed inputs preserved at: $failed_target"
    exit $rc
fi
