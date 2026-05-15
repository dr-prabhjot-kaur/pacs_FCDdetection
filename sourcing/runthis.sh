#!/bin/bash
# Sync sourcing/ → deployed locations on rainbow + HPC, then verify.
# Run from ~/work/gits/dicomtools-pacs/sourcing/

set -e
cd "$(dirname "$0")"

HPC_USER=ch239656@e3-login
HPC_REPO=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
RAINBOW_TOOLS=/home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD

echo "=== Pushing HPC files ==="
rsync -av hpc/process_series.sh   $HPC_USER:$HPC_REPO/scripts/process_series.sh
rsync -av hpc/predict.sh          $HPC_USER:$HPC_REPO/meld_graph/predict.sh
rsync -av hpc/organizeinputs.py   $HPC_USER:$HPC_REPO/organizeinputs/organizeinputs.py
rsync -av hpc/rules.json          $HPC_USER:$HPC_REPO/organizeinputs/rules.json
ssh $HPC_USER "chmod +x $HPC_REPO/scripts/process_series.sh $HPC_REPO/meld_graph/predict.sh"

echo
echo "=== Pushing rainbow files ==="
cp rainbow/submitter.py $RAINBOW_TOOLS/submitter.py

echo
echo "=== Verifying deployed contents ==="

check() {
    local name="$1"; local pattern="$2"; local path="$3"
    printf "  %-55s" "$name"
    if grep -q "$pattern" "$path" 2>/dev/null; then
        echo "ok"
    else
        echo "MISSING"
    fi
}

check_remote() {
    local name="$1"; local pattern="$2"; local path="$3"
    printf "  %-55s" "$name"
    if ssh $HPC_USER "grep -q '$pattern' '$path'" 2>/dev/null; then
        echo "ok"
    else
        echo "MISSING"
    fi
}

# Rainbow checks
check "rainbow submitter.py — TZ fix:" \
      "time.time() - submitted_marker" \
      "$RAINBOW_TOOLS/submitter.py"
check "rainbow submitter.py — touch fix:" \
      "submitted.touch()" \
      "$RAINBOW_TOOLS/submitter.py"
check "rainbow submitter.py — silent dead-marker check:" \
      "if marker_path.exists():" \
      "$RAINBOW_TOOLS/submitter.py"
check "rainbow submitter.py — meld_graph stage 2:" \
      "Stage 2 (GPU): meld_graph" \
      "$RAINBOW_TOOLS/submitter.py"

echo
# HPC checks
check_remote "HPC process_series.sh — meld_graph_gpu.sif:" \
             "meld_graph_gpu.sif" \
             "$HPC_REPO/scripts/process_series.sh"
check_remote "HPC process_series.sh — nnunet DISABLED:" \
             "DISABLED" \
             "$HPC_REPO/scripts/process_series.sh"
check_remote "HPC predict.sh — cleanenv:" \
             "cleanenv" \
             "$HPC_REPO/meld_graph/predict.sh"
check_remote "HPC predict.sh — MELD_LICENSE env path:" \
             "APPTAINERENV_MELD_LICENSE=/data" \
             "$HPC_REPO/meld_graph/predict.sh"
check_remote "HPC predict.sh — gpu sif default:" \
             "meld_graph_gpu.sif" \
             "$HPC_REPO/meld_graph/predict.sh"
check_remote "HPC organizeinputs.py — CSV writer:" \
             "def write_series_csv" \
             "$HPC_REPO/organizeinputs/organizeinputs.py"
check_remote "HPC organizeinputs.py — CSV write call:" \
             "Wrote series inventory CSV" \
             "$HPC_REPO/organizeinputs/organizeinputs.py"
check_remote "HPC organizeinputs.py — per-rule miss reasons:" \
             "_rule_matches_with_reasons" \
             "$HPC_REPO/organizeinputs/organizeinputs.py"
check_remote "HPC rules.json — Siemens-only T1 rules:" \
             "Siemens_MPRAGE_3D" \
             "$HPC_REPO/organizeinputs/rules.json"
check_remote "HPC rules.json — Siemens-only FLAIR rules:" \
             "Siemens_3D_FLAIR_SPACE" \
             "$HPC_REPO/organizeinputs/rules.json"

echo
echo "=== Done ==="
echo "** Restart submitter daemon (Ctrl+C Terminal 2 + relaunch) for code changes to take effect. **"
echo "** No restart needed for HPC scripts — they're invoked fresh per sbatch. **"
