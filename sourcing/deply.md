# Deployment bundle — pacs_FCDdetection (meld_graph only)

Three files, two machines. Replace each at the destination shown.

---

## What's in this bundle

```
deploy/
├── rainbow/
│   └── submitter.py          ← workstation
└── hpc/
    ├── process_series.sh     ← HPC, called by submitter via sbatch
    └── predict.sh            ← HPC, called by process_series.sh as separate sbatch
```

---

## Where each file goes

| Bundle file | Destination |
|---|---|
| `rainbow/submitter.py` | **Rainbow:** `/home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD/submitter.py` |
| `hpc/process_series.sh` | **HPC:** `/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh` |
| `hpc/predict.sh` | **HPC:** `/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh` |

That's it. **Three target paths, period.**

Other copies that exist on disk (`organizeinputs/process_series.sh`, `processseries.sh`, `process_series_beforeMELD.sh`, `container_byprabhjot/predict.sh`) are dev/backup. Leave them alone.

---

## Deploy commands

### On rainbow workstation

```bash
# Backup current submitter (just in case)
cp /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD/submitter.py \
   /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD/submitter.py.bak.$(date +%Y%m%d_%H%M%S)

# Replace (assumes you've downloaded submitter.py into /tmp)
cp /tmp/submitter.py \
   /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD/submitter.py

# Verify
grep -c "Stage 2 (GPU): meld_graph" \
   /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD/submitter.py
# Expect: 1
```

### On HPC

```bash
# Backups
cp /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh \
   /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh.bak.$(date +%Y%m%d_%H%M%S)

cp /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh \
   /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh.bak.$(date +%Y%m%d_%H%M%S)

# Replace (assumes you've downloaded both into /tmp)
cp /tmp/process_series.sh /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh
cp /tmp/predict.sh        /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh

chmod +x /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh
chmod +x /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh

# Verify
echo "=== process_series.sh ==="
grep -c "DISABLED" /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh        # expect: 1
grep -c "meld_graph_gpu.sif" /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh  # expect: 1

echo "=== predict.sh ==="
grep -c "cleanenv" /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh                # expect: 1+
grep -c "APPTAINERENV_MELD_LICENSE=/data" /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh  # expect: 1
grep -c "meld_graph_gpu.sif" /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/meld_graph/predict.sh     # expect: 1
```

If all greps return the expected counts, deploy is complete.

---

## After deploy: run the three terminals

Per your existing README pattern. (storagescp + submitter run on rainbow, foreground.)

### Terminal 1 — storagescp

```bash
cd /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD
LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom

python3 storagescp.py \
  --incoming-dir       $LOCAL/in \
  --study-pending-dir  $LOCAL/study_pending \
  --staging-dir        $LOCAL/staging \
  --queue-dir          $LOCAL/queue \
  --orphaned-dir       $LOCAL/orphaned \
  --quarantine-dir     $LOCAL/quarantine \
  --archive-dir        $LOCAL/archive \
  --failed-dir         $LOCAL/failed \
  --timeout-minutes    1
```

**Note: `--timeout-minutes 1` for testing** (was 20 in production). Watcher promotes the study 1 min after first DICOM arrives instead of 20 min.

### Terminal 2 — submitter

```bash
cd /home_local/ch239656/work/gits/dicomtools-pacs/dicom-tools/storagescp_withMELD
LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom
TRANSIT_WS=/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
HPC_SCRIPT=/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scripts/process_series.sh

python3 submitter.py \
  --staging-dir         $LOCAL/staging \
  --queue-dir           $LOCAL/queue \
  --out-dir             $LOCAL/out \
  --archive-dir         $LOCAL/archive \
  --failed-dir          $LOCAL/failed \
  --scratch-dir         $TRANSIT_WS/scratch \
  --scratch-archive-dir $TRANSIT_WS/scratch_archive \
  --scratch-failed-dir  $TRANSIT_WS/scratch_failed \
  --ssh-host            ch239656@e3-login \
  --sbatch-script       $HPC_SCRIPT \
  --workstation-prefix  /fileserver \
  --hpc-prefix          /lab-share \
  --shared-group        RC_Rad-Warfield-e2_Imp-Recons \
  --poll-interval       30 \
  --recovery-stale-min  10 \
  --slurm-check-interval 300
```

### Terminal 3 — send DICOMs

(Use your unzip-and-send script for the Flywheel subject, or any other test study.)

---

## Expected pipeline trace

```
T+0:00  dcmsend completes  →  storagescp accepts, .first_arrival recorded
T+1:00  watcher promotes   →  staging/<MRN>/<DOS>/, queue/<key>.ready
T+1:01  submitter rsyncs to /fileserver/scratch/<key>/input/, sbatches process_series.sh
        marker → .submitted
T+1:02  process_series.sh starts on bch-compute
        organizeinputs.py runs (~2-5 min)
        → .organize_done
T+1:05  process_series.sh dispatches sbatch meld_graph/predict.sh
        meld-graph-<key> queued on bch-gpu
T+1:10  predict.sh starts FastSurfer (~30-60 min)
T+2:00  feature extraction + harmonisation + graph inference (~10 min)
T+2:10  predictions written, .meld_graph_done touched
T+2:11  submitter polls, sees both done, rsyncs back to /home_local/.../out/
        archives staging + scratch
        marker → .done_<ts>
```

Total: ~2-2.5 hours start-to-finish.

---

## What's in each file

### `submitter.py` (rainbow)

Two-stage classification: organize + meld_graph. nnunet markers ignored.

### `process_series.sh` (HPC `scripts/`)

CPU sbatch script. Runs organizeinputs.py, then dispatches `meld_graph/predict.sh` as a separate GPU sbatch. nnunet dispatch is commented out.

### `predict.sh` (HPC `meld_graph/`)

GPU sbatch wrapper. All four fixes baked in:
1. Default sif = `containers/meld_graph_gpu.sif` (GPU image)
2. Site harmonisation copy from central `MELD_H52/` dir
3. FreeSurfer license placed at `/data/license.txt`
4. MELD_LICENSE env var = `/data/meld_license.txt` (path, not content)
5. `--cleanenv` flag prevents host TMPDIR leaking into container
