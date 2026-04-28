# DICOM pipeline: local workstation + HPC processing

## Architecture

```
 WORKSTATION (/home_local/...)         /fileserver/ (transit)         HPC (/lab-share/...)
 ─────────────────────────             ────────────────────           ───────────────────

 PACS -> storagescp.py
   saves in/<uid>/
   on release: mv -> staging/<uid>/
               touch queue/<uid>.ready

 submitter.py
   1. rsync staging/<uid>/ ────────> scratch/<uid>/input/
   2. ssh sbatch ─────────────────────────────────────────> sbatch queues & runs
                                                             reads  scratch/<uid>/input/
                                                             writes scratch/<uid>/output/
                                                             touches scratch/<uid>/.done
                                                                      │
   3. poll for .done  <──────────── scratch/<uid>/.done ─────────────┘
   4. rsync output <──────────────── scratch/<uid>/output/
      mv staging/<uid> -> archive/<uid>_<ts>/
      mv scratch/<uid> -> scratch_archive/<uid>_<ts>/
      rename queue/<uid>.submitted -> .done_<ts>
```

**Nothing is ever deleted.** All cleanup is a move. `/fileserver/` is
transit only; `/home_local/` holds the long-term archive on the
workstation.

## Why the permissions setup matters

The workstation and HPC see your account as **different numeric UIDs**
(workstation UID 10820 vs HPC UID 2315512), but they share the group
`RC_Rad-Warfield-e2_Imp-Recons` (gid 2264598). Files on shared storage
are bridged through that group. Anything created on `/fileserver/`
that HPC compute nodes need to write must be:

- group-owned by `RC_Rad-Warfield-e2_Imp-Recons`
- group-writable
- and have the **setgid bit** set so child files/dirs inherit the group

This is enforced three ways:

1. **Filesystem setgid bit** on the transit directory tree (one-time setup below).
2. **Shell umask 002** so new files default to `664` and dirs to `775`.
3. **The submitter** explicitly chmods `02775` and chgrps to the shared group every time it creates a transit dir.

If any one of those three fails, the other two recover. If all three are wrong, HPC will get `Permission denied` errors and slurm will kill the job before any logs get written.

## Directory layout

### Workstation (long-term data home)

Under `/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom/`:

```
in/         SCP receive buffer. Ephemeral. Same FS as staging/.
staging/    Fully-received series awaiting or undergoing processing.
queue/      Marker files tracking per-series state.
out/        Final processor outputs (pulled back from HPC).
archive/    Raw series preserved after successful processing.
failed/     Raw series preserved after failed processing.
orphaned/   Series left in in/ from a crashed SCP; swept on startup.
```

### Fileserver (transit scratch — visible to both workstation and HPC)

Under `/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/`:
(HPC sees the same at `/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/`)

```
scratch/                Per-job working dirs. Each has input/, output/, job.log.
scratch_archive/        Completed scratch dirs (timestamped). Contains HPC-side
                        inputs, outputs, and slurm job logs for full audit trail.
scratch_failed/         Same, but for failed jobs.
data/slurm-logs/        Slurm stdout/stderr per job.
scripts/process_series.sh   The sbatch script.
scripts/process_stub.py     Placeholder processor (replace with real code).
```

## Files in this repo

- `storagescp.py` — DICOM Storage SCP, runs on the workstation. Receives C-STORE, hands off completed series.
- `submitter.py` — Async submitter, runs on the workstation. Pushes data to scratch, calls sbatch, polls for `.done`/`.failed`, pulls back results, archives.
- `process_series.sh` — sbatch script. Runs on HPC. Activates the conda env via PATH (no `conda activate`), runs the processor, touches `.done` or `.failed`.
- `process_stub.py` — Placeholder processor. Reads DICOMs, writes summary.json. Replace with your real processor when ready.

---

## ONE-TIME SETUP

### Step 1 — Set umask in your shell

Add to `~/.bashrc` so every new terminal has it:

```bash
echo 'umask 002' >> ~/.bashrc
```

Then reload current shell:
```bash
source ~/.bashrc
umask   # should print 0002
```

### Step 2 — Create local directories (workstation)

```bash
LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom
mkdir -p $LOCAL/{in,staging,queue,out,archive,failed,orphaned}
```

### Step 3 — Create transit directories (fileserver)

```bash
TRANSIT=/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
mkdir -p $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed
mkdir -p $TRANSIT/data/slurm-logs
mkdir -p $TRANSIT/scripts
```

### Step 4 — Apply group ownership + setgid to transit dirs

```bash
chgrp -R RC_Rad-Warfield-e2_Imp-Recons \
  $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed \
  $TRANSIT/data/slurm-logs $TRANSIT/scripts

chmod -R g+rwXs \
  $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed \
  $TRANSIT/data/slurm-logs $TRANSIT/scripts

ls -la $TRANSIT/
```

You should see `drwxrwsr-x ch239656 RC_Rad-Warfield-e2_Imp-Recons` on every subdir. The trailing `s` is the setgid bit.

### Step 5 — Place scripts on shared storage

```bash
cd ~/work/gits/dicomtools-pacs/dicom-tools/storagescp

cp process_series.sh $TRANSIT/scripts/
cp process_stub.py   $TRANSIT/scripts/
chmod +x $TRANSIT/scripts/process_series.sh

ls -la $TRANSIT/scripts/
```

### Step 6 — Verify HPC can write into a scratch subdir

```bash
mkdir $TRANSIT/scratch/perm-test

ssh ch239656@e3-login "touch /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch/perm-test/from-hpc.txt && echo SUCCESS || echo FAILED"

ls -la $TRANSIT/scratch/perm-test/
rm -rf $TRANSIT/scratch/perm-test
```

Must print `SUCCESS`. If it prints `FAILED`, the setgid bit didn't take — re-run Step 4 and check that your shell's umask is 002.

### Step 7 — Verify HPC env

```bash
ssh ch239656@e3-login "/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/fcd4pacs/MAP/fcd_env/bin/python3 -c 'import pydicom; print(pydicom.__version__)'"
```

Should print a version (e.g., `2.4.4`). The sbatch script uses this exact path; no `conda activate` needed.

---

## RUNNING

Three terminals on the workstation. **Make sure each shell has `umask 002` before starting** (it's set automatically if you added it to `~/.bashrc` in Step 1).

### Terminal 1 — SCP

```bash
cd ~/work/gits/dicomtools-pacs/dicom-tools/storagescp
LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom

python3 storagescp.py \
  --incoming-dir $LOCAL/in \
  --staging-dir  $LOCAL/staging \
  --queue-dir    $LOCAL/queue \
  --orphaned-dir $LOCAL/orphaned
```

Expected startup:
```
SCP listening on 127.0.0.1:11112 (AE title: ANY-SCP)
```

### Terminal 2 — submitter

```bash
cd ~/work/gits/dicomtools-pacs/dicom-tools/storagescp
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
  --poll-interval       30
```

Expected startup:
```
Shared group: RC_Rad-Warfield-e2_Imp-Recons (gid 2264598)
Submitter started. Polling every 30.0s.
```

### Terminal 3 — send a DICOM

```bash
cd ~/work/gits/dicomtools-pacs/data_fromanotherterminal

docker run --network=host --rm -v $(pwd):/data \
  prabhjotkaur1991/dicom-tools-prabh:latest \
  dcmsend --verbose --scan-directories 127.0.0.1 11112 -aet COMPRES /data/
```

---

## EXPECTED OUTPUT

**Terminal 1 (SCP):**
```
Association with COMPRES released. Series received: 1
  Staged <uid>, marker written
```

**Terminal 2 (submitter), within ~30s:**
```
Launching <uid>
  rsync -> scratch/<uid>/input/
  sbatch on HPC: /lab-share/.../scratch/<uid>
  Slurm job NNNNN submitted
  -> submitted
```

**Terminal 2, ~30-60s later (after the job finishes on HPC):**
```
Completion detected: <uid> (success)
  rsync output -> local out/<uid>/
  local staging -> /home_local/.../archive/<uid>_<ts>
  scratch -> /fileserver/.../scratch_archive/<uid>_<ts>
```

**Verify:**
```bash
ls $LOCAL/archive/      # <uid>_<ts>/ with raw DICOMs
ls $LOCAL/out/          # <uid>/ with summary.json, manifest.txt
ls $LOCAL/queue/        # <uid>.done_<ts>
cat $LOCAL/out/*/summary.json
```

---

## MONITORING

HPC job state:
```bash
ssh ch239656@e3-login "squeue -u \$USER"
```

Slurm logs:
```bash
ssh ch239656@e3-login "ls -ltr /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/ | tail"
ssh ch239656@e3-login "cat /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/<jobid>.out"
```

Per-job log captured by the sbatch script:
```bash
ls /fileserver/.../pacs_FCDdetection/scratch/<uid>/job.log
ls /fileserver/.../pacs_FCDdetection/scratch_archive/<uid>_<ts>/job.log   # after completion
```

Local queue state:
```bash
ls /home_local/ch239656/work/gits/dicomtools-pacs/data_dicom/queue/
```

### Marker meanings

| Filename                     | Meaning                              |
|------------------------------|--------------------------------------|
| `<uid>.ready`                | waiting for submitter                |
| `<uid>.submitting`           | submitter is pushing + submitting    |
| `<uid>.submitted`            | sbatch accepted, job running on HPC  |
| `<uid>.done_<ts>`            | success, results in out/<uid>/       |
| `<uid>.failed_<ts>`          | job reported failure                 |
| `<uid>.submit-failed_<ts>`   | sbatch itself rejected               |

---

## RESET / CLEANUP

If you need to start over (fresh tests, broken state, whatever):

### Stop running processes
Ctrl+C in Terminal 1 (SCP) and Terminal 2 (submitter).

### Clean local state (workstation)
```bash
LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom
rm -rf $LOCAL/in/* $LOCAL/staging/* $LOCAL/queue/* \
       $LOCAL/out/* $LOCAL/archive/* $LOCAL/failed/* $LOCAL/orphaned/*
```

### Clean transit state (some files owned by HPC UID — delete from HPC)
```bash
ssh ch239656@e3-login "rm -rf \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch/* \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch_archive/* \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch_failed/* \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/*"
```

### Re-apply group + setgid to transit dirs
```bash
TRANSIT=/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
chgrp -R RC_Rad-Warfield-e2_Imp-Recons $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed $TRANSIT/data/slurm-logs $TRANSIT/scripts
chmod -R g+rwXs $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed $TRANSIT/data/slurm-logs $TRANSIT/scripts
```

---

## TROUBLESHOOTING

### Slurm job fails with `ExitCode 0:53`, no logs written

The sbatch script died at setup time. Common causes:
- Permission denied creating `output/` or `job.log` (most common — see "Why permissions matter" above).
- `set -u` plus an undefined env var.
- Missing `--output` / `--error` directory.

**Diagnose:**
```bash
# Run the sbatch script directly on the login node (bypasses slurm)
ssh ch239656@e3-login "bash /lab-share/.../scripts/process_series.sh /lab-share/.../scratch/<uid>"
```
The output will show exactly what fails.

### `dcmsend` aborts with "No Acceptable Presentation Contexts"

You're sending a `DICOMDIR` index file or another non-storage SOP class. Use `--scan-directories --scan-pattern "*.dcm"` to filter to actual DICOM images.

### `dcmsend` aborts with "SOP Class UID in dataset differs from the one in the transfer list"

A file in your sending directory is corrupt or not a real DICOM. Add `--no-halt` to skip bad files instead of aborting the whole batch.

### emf2sf rejects with "Missing (5200,9229) Shared Functional Groups Sequence"

The DICOM file claims to be Enhanced MR but doesn't have the required functional groups sequences. This is data-side, not pipeline-side. The pipeline correctly captured the failure.

### `Address already in use` on SCP startup

A previous SCP is still running. Kill it:
```bash
pkill -f storagescp.py
```

---

## SWAPPING IN THE REAL PROCESSOR

Edit `$TRANSIT/scripts/process_series.sh`. Find:
```bash
PROCESSOR="${PROCESSOR:-python3 $SCRIPTS_DIR/process_stub.py}"
```

Replace with your real processor invocation. The contract is just:
```
your-processor --in-dir <dicom_input_dir> --out-dir <results_output_dir>
```

Everything else (env activation via PATH, archiving, failure handling, marker management) stays the same.

---

## PARALLELISM

Multiple DICOM studies arriving in rapid succession are each submitted
independently; Slurm runs them concurrently on different nodes. The
submitter does not block between submissions — it launches each new
`.ready` marker immediately and polls all in-flight jobs in the same
loop cycle.

---

## RETENTION

`archive/`, `failed/`, `scratch_archive/`, `scratch_failed/`, old queue
markers, and slurm logs all grow unbounded. Run a separate prune job
on a schedule when you're ready — keeping it separate from the live
pipeline means pruning bugs can't take out in-flight work.
