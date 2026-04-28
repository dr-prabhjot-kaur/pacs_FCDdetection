# DICOM pipeline: study-level (MRN + DOS) processing on HPC

A receive-and-process pipeline. PACS sends DICOMs to a workstation,
which receives them, **groups them by MRN + StudyDate**, waits 20
minutes from the first arrival of each study, then ships the whole
study to HPC for processing.

---

## Architecture

```
 WORKSTATION (/home_local/...)         /fileserver/ (transit)         HPC (/lab-share/...)
 ─────────────────────────             ────────────────────           ───────────────────

 PACS -> storagescp.py
   handle_store: in/<seriesUID>/
   handle_released: read DICOM -> (MRN, DOS)
                    mv in/<seriesUID> -> study_pending/<MRN>/<DOS>/<seriesUID>/
                    on first series of (MRN,DOS): write .first_arrival timestamp

 background watcher thread (in SCP process):
   every 60s, scan study_pending/<MRN>/<DOS>/
   if .first_arrival is older than 20 min:
     mv -> staging/<MRN>/<DOS>/
     touch queue/<MRN>_<DOS>.ready

 submitter.py:
   1. rsync staging/<MRN>/<DOS>/ ────────> scratch/<MRN>_<DOS>/input/
   2. ssh sbatch ─────────────────────────────────────────> Slurm queues & runs
                                                             reads  scratch/.../input/
                                                             writes scratch/.../output/
                                                             touches scratch/.../.done
                                                                      │
   3. poll for .done  <──────────── scratch/.../.done ──────────────┘
   4. rsync output <──────────────── scratch/.../output/
      mv staging/<MRN>/<DOS>     -> archive/<MRN>_<DOS>_<ts>/
      mv scratch/<MRN>_<DOS>     -> scratch_archive/<MRN>_<DOS>_<ts>/
      rename queue/<MRN>_<DOS>.submitted -> .done_<ts>
```

Three machines, three roles:

- **Workstation** receives DICOMs, groups them into studies, and runs the orchestration logic.
- **`/fileserver/`** is shared scratch — used purely as transit, both workstation and HPC see it (HPC sees it as `/lab-share/`).
- **HPC** runs the heavy compute. Each sbatch job processes one entire study (all series for one MRN + one StudyDate).

---

## Flow explained

The pipeline has three independent processes: an SCP, a watcher thread (inside the SCP process), and a submitter. They communicate via marker files. A study is the unit of work.

**Phase 1 — Receive.** The SCP listens on port 11112. PACS sends DICOM files. `handle_store` saves each file to `in/<seriesUID>/<sopUID>`. The SCP doesn't process anything yet.

**Phase 2 — Group by study.** When PACS releases an association, `handle_released` reads one file from each newly-arrived series to extract `(MRN, StudyDate)`. It moves the series from `in/` into `study_pending/<MRN>/<DOS>/<seriesUID>/`. If this is the first series for that study, it records the arrival time in `.first_arrival`.

**Phase 3 — Wait for the study to settle.** A background watcher thread inside the SCP process scans `study_pending/` every 60 seconds. For each `<MRN>/<DOS>` directory whose `.first_arrival` is older than **20 minutes**, the watcher promotes it: moves the whole tree to `staging/<MRN>/<DOS>/` and writes `queue/<MRN>_<DOS>.ready`.

**Phase 4 — Hand off to HPC.** The submitter polls `queue/`, sees the `.ready` marker, atomically claims it, rsyncs the staging tree to `/fileserver/scratch/<MRN>_<DOS>/input/`, and SSHes to HPC to call `sbatch process_series.sh /lab-share/.../scratch/<MRN>_<DOS>`. Slurm queues the job. The marker becomes `.submitted`. Multiple studies can be in flight simultaneously, processed in parallel on different HPC nodes.

**Phase 5 — HPC processing.** The sbatch script activates the conda env via PATH, invokes the processor on `input/` (which contains all series for the study), and `touch`es `.done` on success or `.failed` on failure. The processor handles modality classification, validation, and whatever logic is needed.

**Phase 6 — Pull back and archive.** The submitter polls every 30 seconds for `.done`/`.failed` markers on `/fileserver/scratch/`. When one appears, it rsyncs `output/` to local `out/<MRN>/<DOS>/`, moves local `staging/<MRN>/<DOS>/` to `archive/<MRN>_<DOS>_<ts>/`, moves the scratch dir to `scratch_archive/<MRN>_<DOS>_<ts>/`, and renames the queue marker to `.done_<ts>` or `.failed_<ts>`.

---

## Flowchart

```
                  ┌──────────────────────┐
                  │  PACS C-STORE batch  │
                  └──────────┬───────────┘
                             │
                             ▼
                ┌────────────────────────────┐
                │  handle_store              │
                │  in/<seriesUID>/<sopUID>   │
                └────────────┬───────────────┘
                             │ association releases
                             ▼
                ┌────────────────────────────┐
                │  handle_released           │
                │  read DICOM -> (MRN, DOS)  │
                │  in/<seriesUID> ->         │
                │    study_pending/<MRN>/    │
                │      <DOS>/<seriesUID>/    │
                │  set .first_arrival if new │
                └────────────┬───────────────┘
                             │
              ┌──────────────┴──────────────┐
              │ same MRN+DOS already        │
              │ in flight or completed?     │
              ├──── YES ────► quarantine    │
              │                             │
              └──── NO  ────► study_pending │
                                            │
                  ┌─────────────────────────┘
                  │
                  ▼
        ┌─────────────────────────────┐
        │  watcher thread (every 60s) │
        │  for each <MRN>/<DOS>:      │
        │    .first_arrival > 20 min? │
        ├─── NO ──► keep waiting      │
        └─── YES ─► promote           │
                  │
                  ▼
        ┌─────────────────────────────┐
        │  staging/<MRN>/<DOS>/       │
        │  queue/<MRN>_<DOS>.ready    │
        └────────────┬────────────────┘
                     │
                     ▼
        ┌─────────────────────────────┐
        │  submitter:                 │
        │  rsync staging -> scratch   │
        │  ssh sbatch                 │
        │  marker -> .submitted       │
        └────────────┬────────────────┘
                     │
       ┌─────────────┴─────────────┐
       │ HPC + submitter polling   │
       │  (parallel)               │
       ▼                           ▼
   ┌──────────────────┐   ┌─────────────────────┐
   │  Slurm runs job  │   │ submitter polls 30s │
   │  reads input/    │   │ for .done / .failed │
   │  writes output/  │   │                     │
   │  touches .done   │──►│                     │
   │       OR .failed │   │                     │
   └──────────────────┘   └─────────┬───────────┘
                                    │
                ┌───────────────────┴───────────────────┐
                │                                       │
                ▼                                       ▼
       ┌────────────────────┐                ┌────────────────────┐
       │ .done detected:    │                │ .failed detected:  │
       │  rsync output back │                │  no rsync          │
       │  to local out/     │                │                    │
       └─────────┬──────────┘                └─────────┬──────────┘
                 │                                     │
                 ▼                                     ▼
       ┌────────────────────┐                ┌────────────────────┐
       │ mv staging/ ->     │                │ mv staging/ ->     │
       │  archive/_<ts>/    │                │  failed/_<ts>/     │
       └─────────┬──────────┘                └─────────┬──────────┘
                 │                                     │
                 ▼                                     ▼
       ┌────────────────────┐                ┌────────────────────┐
       │ mv scratch/ ->     │                │ mv scratch/ ->     │
       │  scratch_archive/  │                │  scratch_failed/   │
       └─────────┬──────────┘                └─────────┬──────────┘
                 │                                     │
                 ▼                                     ▼
       ┌────────────────────┐                ┌────────────────────┐
       │ marker -> .done_ts │                │ marker -> .failed_ts│
       └────────────────────┘                └────────────────────┘
```

---

## Timing

### Single study lifecycle

![Single study timing](timing_single_study.png)

How time flows for one study, from first arrival through archive. The watcher polls every 60s; the submitter polls every 30s; HPC processing time `P_runtime` can be anywhere from seconds (stub) to up to 64 hours (real FCD pipeline). The diagram is not to scale — it compresses the HPC processing block so the orchestration steps remain visible.

The same view in ASCII for terminal/grep-friendliness:

```
Time ──►

Workstation:
  SCP          ●──● ●─●            ●─●
               C-STORE (handle_store: in/<seriesUID>/<sopUID>)
               │
              T0 = first arrival of this study

  Watcher      ····|····|····|····|····●────────────────────────►
               (polls every 60s)       │
                                      T0 + 20min
                                      .first_arrival is now old enough → promote
                                      mv study_pending/<MRN>/<DOS>/ → staging/<MRN>/<DOS>/
                                      touch queue/<MRN>_<DOS>.ready

  Submitter    ··|··|··|··|··|··|··|··●●●─────────────────●──────►
               (polls every 30s)       │ │                │
                                       │ rsync staging → fileserver/scratch/<MRN>_<DOS>/input/
                                       │   marker .ready → .submitting
                                       │ ssh sbatch
                                       │   marker .submitting → .submitted
                                       │
                                       (poll continues; checking for .done/.failed)
                                                          │
                                                          .done detected:
                                                          rsync scratch/output → out/<MRN>/<DOS>/
                                                          mv staging → archive/<MRN>_<DOS>_<ts>/
                                                          mv scratch → scratch_archive/<MRN>_<DOS>_<ts>/
                                                          marker .submitted → .done_<ts>

/fileserver/:
                                       ●────────────────●
                                       scratch/<MRN>_<DOS>/  exists
                                       input/  output/  job.log

HPC:
  Slurm queue                          ●──●
                                       queued→started
  Compute node                              ●──────────●
                                            running:
                                            python3 process_stub.py
                                              --in-dir input/
                                              --out-dir output/
                                                       │
                                                       touch .done

  Time markers:
    T0          first C-STORE for this study arrives
    T0+20min    watcher promotes (absolute timeout)
    T0+~20.5m   submitter rsyncs + sbatches
    T0+~21m     Slurm allocates node, job starts
    T0+~21m+P   processing finishes (P = processor's runtime, up to 64h)
    T0+~21m+P+30s   submitter detects .done, pulls back, archives
```

### Three studies in parallel

![Parallel studies](timing_parallel.png)

Three studies arriving at staggered times. Each is independent end-to-end: the SCP keeps receiving while the watcher promotes others, while the submitter polls in-flight jobs, while Slurm runs concurrent jobs on different compute nodes. The workstation never blocks. Studies finish in whatever order their HPC processing completes, which may differ from submission order.

ASCII version:

```
Time ──►

Study A (MRN1/DOS1):
  ●─●─●───┄┄┄┄ 20min ┄┄┄┄───●────●────●────────────────────●────►
  receive  pending           promote rsync sbatch           done
                                            │
                                            ▼ HPC node 1
                                       [─────P_A──────]

Study B (MRN1/DOS2):
        ●─●─●─┄┄┄┄ 20min ┄┄┄┄──●────●────●─────────────────●──►
        receive pending          promote rsync sbatch        done
                                                │
                                                ▼ HPC node 2
                                              [───P_B───]

Study C (MRN2/DOS1):
                ●─●─●─┄┄┄┄ 20min ┄┄┄┄───●────●────●─────────────────────────●─►
                                          promote rsync sbatch                done
                                                        │
                                                        ▼ HPC node 3
                                                   [────────P_C────────]

  Each study is independent.
  SCP keeps receiving while watcher promotes while submitter polls.
  Slurm runs all jobs concurrently on different nodes.
  Workstation never blocks.
```

---

## Design Choices

The "why" behind decisions. Useful when something needs to change.

### Unit of work = study (MRN, DOS), not series

Earlier the pipeline keyed everything on SeriesInstanceUID. That meant T1, T2, FLAIR for the same patient/session became **three independent sbatch jobs**, each seeing only one series. For FCD detection — which needs multi-modal input — that's wrong. Now the pipeline groups all series by (MRN, StudyDate) and ships the whole study as one unit. Your processor sees a directory of series and decides what to do with them.

### Absolute 20-minute timeout from first arrival

The watcher fires a study once `.first_arrival` is older than 20 min. **The clock does not reset on later arrivals.** Pros: predictable — every study waits exactly 20 min before processing. Cons: anything arriving after 20 min is "late" and gets quarantined. PACS bursts are typically much faster than 20 min, so this should rarely bite. If it becomes a problem, the timeout is configurable via `--timeout-minutes`.

### Quiet timeout vs absolute timeout

We chose absolute. Quiet timeout (clock resets per arrival) is more forgiving but unbounded — a slowly-drip-feeding PACS could hold a study indefinitely. Absolute is bounded and predictable. The trade-off accepted: late arrivals become quarantine cases that need manual review.

### Pipeline doesn't classify modalities

The workstation pipeline is modality-agnostic. It groups by (MRN, DOS) and ships every series in that directory. **The HPC processor handles classification.** This keeps the pipeline simple and means swapping processors (FCD detection today, something else tomorrow) doesn't require pipeline changes. The processor reads `SeriesDescription` / `ProtocolName` / TR-TE-TI to figure out what's what.

### "Study completeness" is the processor's job

If a study fires with only T1 (FLAIR didn't arrive), the processor sees that and returns rc=1. The job lands in `failed/`. The pipeline doesn't try to validate which modalities are required — that's policy that lives with the processor. We may add a "missing modality" status file written by the processor so a future retry script can distinguish "missing inputs" from "real failure."

### Late arrivals → quarantine

A new series for an MRN+DOS that has already been promoted, submitted, completed, or failed is moved to `quarantine/`. This is conservative — preserves the data for manual review without trying to merge it into already-archived work. If late arrivals happen often, we can revisit.

### Markers as queue, not Redis

Each `.ready` / `.submitting` / `.submitted` / `.done_<ts>` file in `queue/` is the queue. Atomic renames make claims race-free. Markers survive crashes. Inspectable with `ls`. Filesystem markers are sufficient for this workload.

### `handle_released` does the bare minimum

The SCP's release handler reads one DICOM per newly-arrived series, moves files, and updates a timestamp. It does not block, does not call HPC, does not run the watcher. Anything heavier belongs in the watcher thread or HPC.

### Watcher inside SCP process, not separate daemon

Started as a thread alongside the SCP. Fewer moving parts than a separate daemon. If the SCP dies, the watcher dies with it — but at restart, leftover `study_pending/<MRN>/<DOS>/.first_arrival` markers are still on disk, so studies whose timeout already expired during downtime will fire on the next watcher cycle.

### Filesystem markers signal HPC completion

The HPC job touches `.done` or `.failed` on shared storage. The submitter checks the filesystem, not slurm. No SSH per poll cycle, no parsing slurm output, no race between "slurm says done" and "results actually written" (the marker is the last thing the job writes).

### Scratch on `/fileserver/`, not HPC `$TMPDIR`

Compute-node `$TMPDIR` is faster I/O but local. Shared scratch lets the workstation rsync data in and pull results out without HPC needing to copy anything itself.

### Long-term archive on workstation

Final outputs live in `/home_local/.../out/<MRN>/<DOS>/`. Raw inputs are archived locally too. `/fileserver/` is purely transit. Scratch_archive on `/fileserver/` keeps HPC-side artifacts (input copy, output, logs) for audit but can be pruned.

### No `rm`, ever

All cleanup is `mv`. Failed jobs preserve everything. Successful jobs preserve raw inputs (in `archive/`) and HPC artifacts (in `scratch_archive/`). The only "deletion" would be a separate retention script you write later.

### Identity bridge via shared Unix group

Workstation UID 10820 and HPC UID 2315512 share group `RC_Rad-Warfield-e2_Imp-Recons`. Files on `/fileserver/` are bridged via the group. Three layers of defense:
1. Filesystem **setgid bit** on transit dirs (one-time).
2. Shell **`umask 002`** so new files default group-writable.
3. **Submitter explicitly chmods/chgrps** anything it creates.

### Direct-PATH Python, not `conda activate`

`conda activate` requires `~/.bashrc` integration which often doesn't run in non-interactive sbatch shells. Setting `PATH=$CONDA_ENV/bin:$PATH` is identical in effect and just works.

### Workstation is the network endpoint

PACS connects to the workstation, not HPC. Keeps HPC simple, keeps the workstation as source of truth.

---

## Directory layout

### Workstation (long-term data home)

Under `/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom/`:

```
in/                           SCP receive buffer. Indexed by SeriesInstanceUID.
                              Ephemeral. Same FS as study_pending/.

study_pending/                Per-study assembly area.
  └── <MRN>/<DOS>/
       ├── .first_arrival     timestamp of first series for this study
       └── <SeriesUID>/<SOPUID>...   one subdir per series

staging/                      Promoted from study_pending after timeout.
  └── <MRN>/<DOS>/
       └── <SeriesUID>/<SOPUID>...

queue/                        Marker files. Flat, named <MRN>_<DOS>.<state>
out/<MRN>/<DOS>/              Final outputs from HPC.
archive/<MRN>_<DOS>_<ts>/     Raw study preserved after success.
failed/<MRN>_<DOS>_<ts>/      Raw study preserved after failure.
quarantine/                   Late arrivals, malformed DICOMs.
orphaned/                     Series left in in/ from a crashed SCP.
```

### Fileserver (transit, both sides)

Under `/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/`:
(HPC sees the same at `/lab-share/...`)

```
scratch/<MRN>_<DOS>/
  ├── input/<SeriesUID>/...    rsynced from staging
  ├── output/                   processor writes here
  ├── job.log                   tee'd from the sbatch script
  ├── .done                     OR
  └── .failed

scratch_archive/<MRN>_<DOS>_<ts>/   completed scratch dirs
scratch_failed/<MRN>_<DOS>_<ts>/    failed scratch dirs
data/slurm-logs/<jobid>.{out,err}   slurm-side logs
scripts/process_series.sh           sbatch script
scripts/process_stub.py             placeholder processor
```

---

## Files

- `storagescp.py` — DICOM SCP. Receives, groups by (MRN, DOS), runs the timeout watcher.
- `submitter.py` — Async submitter. Pushes to HPC, polls for completion, pulls back, archives.
- `process_series.sh` — Sbatch script. Runs on HPC. 64h time limit. PATH-based python.
- `process_stub.py` — Placeholder study-level processor. Replace with real FCD code.

---

## ONE-TIME SETUP

### Step 1 — Set umask

```bash
echo 'umask 002' >> ~/.bashrc
source ~/.bashrc
umask   # should print 0002
```

### Step 2 — Local directories

```bash
LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom
mkdir -p $LOCAL/{in,study_pending,staging,queue,out,archive,failed,quarantine,orphaned}
```

### Step 3 — Transit directories

```bash
TRANSIT=/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection
mkdir -p $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed
mkdir -p $TRANSIT/data/slurm-logs
mkdir -p $TRANSIT/scripts
```

### Step 4 — Group + setgid on transit dirs

```bash
chgrp -R RC_Rad-Warfield-e2_Imp-Recons \
  $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed \
  $TRANSIT/data/slurm-logs $TRANSIT/scripts

chmod -R g+rwXs \
  $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed \
  $TRANSIT/data/slurm-logs $TRANSIT/scripts

ls -la $TRANSIT/   # subdirs should show drwxrwsr-x ... RC_Rad-Warfield-e2_Imp-Recons
```

### Step 5 — Place scripts on shared storage

```bash
cd ~/work/gits/dicomtools-pacs/dicom-tools/storagescp
cp process_series.sh $TRANSIT/scripts/
cp process_stub.py   $TRANSIT/scripts/
chmod +x $TRANSIT/scripts/process_series.sh
```

### Step 6 — Verify HPC can write into a scratch subdir

```bash
mkdir $TRANSIT/scratch/perm-test
ssh ch239656@e3-login "touch /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch/perm-test/from-hpc.txt && echo SUCCESS || echo FAILED"
ls -la $TRANSIT/scratch/perm-test/
rm -rf $TRANSIT/scratch/perm-test
```

Must print SUCCESS.

### Step 7 — Verify HPC env

```bash
ssh ch239656@e3-login "/lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/fcd4pacs/MAP/fcd_env/bin/python3 -c 'import pydicom; print(pydicom.__version__)'"
```

Should print a version.

---

## RUNNING

Three terminals on the workstation. Each shell needs `umask 002` (auto if Step 1 was done).

### Terminal 1 — SCP

```bash
cd ~/work/gits/dicomtools-pacs/dicom-tools/storagescp
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
  --timeout-minutes    20
```

Expected:
```
Watcher started: timeout=20.0 min, poll=60.0s
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

### Terminal 3 — send a DICOM

```bash
cd ~/work/gits/dicomtools-pacs/data_fromanotherterminal

docker run --network=host --rm -v $(pwd):/data \
  prabhjotkaur1991/dicom-tools-prabh:latest \
  dcmsend --verbose --scan-directories 127.0.0.1 11112 -aet COMPRES /data/
```

---

## EXPECTED OUTPUT

**Terminal 1 (SCP), on association release:**
```
Association with COMPRES released. Series received: 3
  Staged 1.3.12...AAA -> study_pending/12345/20260424/
  New study: 12345/20260424 (first arrival recorded)
  Staged 1.3.12...BBB -> study_pending/12345/20260424/
  Staged 1.3.12...CCC -> study_pending/12345/20260424/
```

**Terminal 1, ~20 minutes later:**
```
Study 12345/20260424 promoted after timeout. Series: 3
    1.3.12...AAA
    1.3.12...BBB
    1.3.12...CCC
  marker: queue/12345_20260424.ready
```

**Terminal 2, within 30s of promotion:**
```
Launching study 12345_20260424
  rsync staging/12345/20260424/ -> scratch/12345_20260424/input/
  sbatch on HPC: /lab-share/.../scratch/12345_20260424
  Slurm job NNNNN submitted
  -> submitted
```

**Terminal 2, when HPC finishes:**
```
Completion detected: 12345_20260424 (success)
  rsync output -> local out/12345/20260424/
  local staging -> .../archive/12345_20260424_<ts>
  scratch -> .../scratch_archive/12345_20260424_<ts>
```

---

## MONITORING

```bash
# Pending studies (waiting for timeout)
ls -la $LOCAL/study_pending/*/*/.first_arrival 2>/dev/null

# In-flight queue state
ls $LOCAL/queue/

# HPC job state
ssh ch239656@e3-login "squeue -u \$USER"

# Slurm logs
ssh ch239656@e3-login "ls -ltr /lab-share/.../pacs_FCDdetection/data/slurm-logs/ | tail"

# Per-study log captured by sbatch
cat /fileserver/.../pacs_FCDdetection/scratch/<MRN>_<DOS>/job.log
```

### Marker meanings

| Filename                         | Meaning                                       |
|----------------------------------|-----------------------------------------------|
| `<MRN>_<DOS>.ready`              | promoted, waiting for submitter               |
| `<MRN>_<DOS>.submitting`         | submitter is pushing + submitting             |
| `<MRN>_<DOS>.submitted`          | sbatch accepted, job running on HPC           |
| `<MRN>_<DOS>.done_<ts>`          | success, results in out/<MRN>/<DOS>/          |
| `<MRN>_<DOS>.failed_<ts>`        | job reported failure                          |
| `<MRN>_<DOS>.submit-failed_<ts>` | sbatch itself rejected                        |

---

## RESET / CLEANUP

```bash
# Stop SCP and submitter (Ctrl+C terminals)

LOCAL=/home_local/ch239656/work/gits/dicomtools-pacs/data_dicom
TRANSIT=/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection

# Local
rm -rf $LOCAL/in/* $LOCAL/study_pending/* $LOCAL/staging/* $LOCAL/queue/* \
       $LOCAL/out/* $LOCAL/archive/* $LOCAL/failed/* \
       $LOCAL/quarantine/* $LOCAL/orphaned/*

# Transit (some files may be HPC-owned)
ssh ch239656@e3-login "rm -rf \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch/* \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch_archive/* \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/scratch_failed/* \
  /lab-share/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/work/gits/pacs_FCDdetection/data/slurm-logs/*"

# Reapply group + setgid
chgrp -R RC_Rad-Warfield-e2_Imp-Recons $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed $TRANSIT/data/slurm-logs $TRANSIT/scripts
chmod -R g+rwXs $TRANSIT/scratch $TRANSIT/scratch_archive $TRANSIT/scratch_failed $TRANSIT/data/slurm-logs $TRANSIT/scripts
```

---

## TROUBLESHOOTING

### Slurm job fails with `ExitCode 0:53`, no logs

Permission denied during sbatch setup. Verify Step 6. Confirm setgid bit is set on the parent scratch dir. Run the script directly on the login node:
```bash
ssh ch239656@e3-login "bash /lab-share/.../scripts/process_series.sh /lab-share/.../scratch/<study>"
```

### Study never promotes from study_pending

Check `.first_arrival`:
```bash
cat $LOCAL/study_pending/<MRN>/<DOS>/.first_arrival
```
Compare against `now()` minus 20 min. Watcher logs every promotion in Terminal 1.

### `dcmsend` "No Acceptable Presentation Contexts"

You're sending a DICOMDIR or non-storage SOP class. Add `--scan-directories --scan-pattern "*.dcm" --no-halt`.

### `Address already in use`

```bash
pkill -f storagescp.py
```

---

## SWAPPING IN THE REAL PROCESSOR

Edit `$TRANSIT/scripts/process_series.sh`:
```bash
PROCESSOR="${PROCESSOR:-python3 $SCRIPTS_DIR/process_stub.py}"
```
to invoke your real FCD detection code. Contract is `--in-dir <study_dir> --out-dir <results_dir>`. Your processor sees a directory of series subdirs (one per modality, named by SeriesInstanceUID).

---

## RETENTION

`archive/`, `failed/`, `quarantine/`, `scratch_archive/`, `scratch_failed/`, old queue markers, slurm logs all grow unbounded. Run a separate prune script on schedule when ready — keeping it separate means pruning bugs can't take out in-flight work.
