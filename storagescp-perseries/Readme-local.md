# Async DICOM receive + HPC processing pipeline

## Architecture

```
             PACS (or dcmsend)
                    │
                    │  C-STORE
                    ▼
        ┌────────────────────────┐
        │     storagescp.py      │    Writes to  in/<uid>/
        │                        │    On release: mv to staging/<uid>/,
        │                        │                touch queue/<uid>.ready
        └────────────────────────┘
                    │
                    │  (queue/<uid>.ready)
                    ▼
        ┌────────────────────────┐
        │     submitter.py       │    Claim: .ready -> .submitting
        │                        │    Submit: sbatch (via ssh / local / dry-run)
        │                        │    Rename: .submitting -> .submitted
        └────────────────────────┘
                    │
                    │  sbatch
                    ▼
        ┌────────────────────────┐
        │   process_series.sh    │    Runs real processor.
        │   (on HPC)             │    On success: staging/<uid>/ -> archive/<uid>_<ts>/
        │                        │                queue/<uid>.submitted -> .done_<ts>
        │                        │    On failure: staging/<uid>/ -> failed/<uid>_<ts>/
        │                        │                queue/<uid>.submitted -> .failed_<ts>
        └────────────────────────┘
```

## Directory layout

Pick a root, e.g. `/shared/dicom`:

```
/shared/dicom/
├── in/         SCP's receive buffer. Ephemeral. Must share fs with staging/.
├── staging/    Fully-received series awaiting or undergoing processing.
├── queue/      Marker files (.ready, .submitting, .submitted, .done_<ts>, .failed_<ts>).
├── out/        Final processor outputs.
├── archive/    Raw series preserved after successful processing (timestamped).
├── failed/     Raw series preserved after failed processing (timestamped).
└── orphaned/   Series left in in/ from a crashed SCP; swept on SCP startup.
```

**Critical:** `in/` and `staging/` must be on the same filesystem so the
handoff is an atomic rename. `staging/`, `out/`, `archive/`, `failed/`,
and `queue/` must be visible to the HPC compute nodes if HPC is the
processing target.

## Quick start — local dry-run (no HPC needed)

This exercises every step except the real sbatch submission.

```bash
ROOT=/tmp/dicom
mkdir -p $ROOT/{in,staging,queue,out,archive,failed,orphaned}

# Terminal 1 — SCP
python3 storagescp.py \
  --incoming-dir $ROOT/in \
  --staging-dir  $ROOT/staging \
  --queue-dir    $ROOT/queue \
  --orphaned-dir $ROOT/orphaned \
  --debug

# Terminal 2 — submitter (dry-run: echoes sbatch command, does nothing)
python3 submitter.py \
  --queue-dir     $ROOT/queue \
  --staging-dir   $ROOT/staging \
  --out-dir       $ROOT/out \
  --archive-dir   $ROOT/archive \
  --failed-dir    $ROOT/failed \
  --sbatch-script /absolute/path/to/process_series.sh \
  --mode dry-run

# Terminal 3 — send something
docker run --network=host --rm -v $(pwd):/data \
  prabhjotkaur1991/dicom-tools-prabh:latest \
  dcmsend --verbose 127.0.0.1 11112 -aet COMPRES /data/yourfile.dcm
```

What you should see:

- Terminal 1 logs C-STORE events, then on association release prints
  `Staged <uid>, marker written` and returns fast.
- Terminal 2 picks up the marker, echoes the sbatch command it *would*
  have run, renames `.ready` → `.submitted`.
- `ls $ROOT/queue` shows `<uid>.submitted`.
- `ls $ROOT/staging/<uid>/` shows the received files.
- `ls $ROOT/in/` is empty.

The marker stays `.submitted` forever in dry-run mode (no job is running
to complete it). That's the stopping point for purely-local testing of
the SCP and submitter shape.

## Local end-to-end (no HPC, simulate the full loop)

If you want to see the full lifecycle including archive/done transitions
without HPC, run the sbatch script manually after submitting:

```bash
# After submitter has renamed marker to .submitted:
bash process_series.sh \
  $ROOT/staging/<uid> \
  $ROOT/out/<uid> \
  $ROOT/queue/<uid>.submitted \
  $ROOT/archive \
  $ROOT/failed
```

You'll see `staging/<uid>/` move to `archive/<uid>_<ts>/`, the marker
rename to `.done_<ts>`, and `out/<uid>/summary.json` appear.

## Connecting to HPC

Three things to change:

1. **Edit `process_series.sh`:**
   - Set the `#SBATCH --output` / `--error` paths to a real slurm-logs dir.
   - Set `PROCESSOR` to invoke your real processor (module loads, venv,
     etc.) or keep `process_stub.py` to start.

2. **Place `process_series.sh` on the HPC filesystem** at a path the
   login node can see. All five args it takes are paths that must be
   readable/writable from HPC compute nodes, so they need to be on
   shared storage.

3. **Run the submitter in SSH mode:**

   ```bash
   python3 submitter.py \
     ... (same dirs as before, but pointing at shared storage) ...
     --sbatch-script /shared/bin/process_series.sh \
     --mode ssh \
     --ssh-host you@login.hpc.example.org
   ```

   Requires passwordless SSH keys to the login node.

## Queue marker states

Watching `queue/` tells you the state of every series:

| Filename                           | Meaning |
|------------------------------------|---------|
| `<uid>.ready`                      | Waiting for submitter to pick up |
| `<uid>.submitting`                 | Submitter is calling sbatch right now |
| `<uid>.submitted`                  | Accepted by Slurm; HPC job queued or running |
| `<uid>.done_<ts>`                  | HPC job finished successfully at `<ts>` |
| `<uid>.failed_<ts>`                | HPC job reported failure at `<ts>` |
| `<uid>.submit-failed_<ts>`         | sbatch itself rejected the job |

Handy inspection:

```bash
ls queue/*.submitted | wc -l          # in-flight jobs
ls queue/*.failed_*                    # failures to triage
ls queue/*.submit-failed_*             # submission-side failures
```

## Retention

Nothing is deleted automatically. `archive/`, `failed/`, and old markers
in `queue/` grow unbounded. Write a separate prune script that runs on
cron and removes things older than N days. Keeping pruning separate
from the live pipeline means a bug in retention can't take out in-flight
work.

## Swapping the processor

When you're ready to replace the stub: edit `process_series.sh` and
change the `PROCESSOR` line to invoke your real code. The only contract
is `--in-dir <staging_series_path> --out-dir <out_series_path>`.
Everything else (archiving, marker transitions, failure handling) stays
the same.

## Duplicate sends

If PACS sends the same SeriesInstanceUID a second time while the first
is still in flight, the new arrival is staged as
`<uid>_resent_<timestamp>/` (with a matching `<uid>_resent_<timestamp>.ready`
marker) rather than overwriting the in-flight data. Both get processed
independently.

## Files

- `storagescp.py`          — receives and hands off
- `submitter.py`           — polls queue and submits sbatch
- `process_series.sh`      — sbatch script (runs on HPC)
- `process_stub.py`        — placeholder processor (replace with real one)
