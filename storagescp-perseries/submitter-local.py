"""DICOM processing submitter.

Polls queue/ for <uid>.ready markers. For each one:

  1. Atomically claims the marker by renaming .ready -> .submitting.
  2. Submits an sbatch job (via SSH, or local, or dry-run).
  3. On accepted submission, renames .submitting -> .submitted.
  4. On submission failure, renames .submitting -> .submit-failed_<ts>
     for manual inspection.

The submitter does NOT wait for the HPC job to finish. The sbatch
script itself is responsible for:
  - reading staging/<uid>/
  - writing out/<uid>/
  - renaming staging/<uid>/ -> archive/<uid>_<ts>/ on success
    or failed/<uid>_<ts>/ on failure
  - renaming queue/<uid>.submitted -> queue/<uid>.done_<ts> or
    .failed_<ts>
"""

import argparse
import datetime as dt
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path


def _timestamp():
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def _claim(marker: Path) -> Path | None:
    """Atomically claim a .ready marker by renaming to .submitting.
    Returns the new path, or None if someone else got it first."""
    claimed = marker.with_suffix(".submitting")
    try:
        marker.rename(claimed)
        return claimed
    except FileNotFoundError:
        return None


def _build_submit_cmd(args, series_uid: str) -> list[str]:
    """Construct the command that submits the job.

    Three modes:
      - dry-run: print what would be submitted, don't actually submit.
      - local:   run sbatch on this machine (useful if SCP host IS an
                 HPC login node, or for testing with local Slurm).
      - ssh:     run sbatch on a remote login node via SSH.
    """
    staging_series = str(Path(args.staging_dir).resolve() / series_uid)
    out_series = str(Path(args.out_dir).resolve() / series_uid)
    archive_root = str(Path(args.archive_dir).resolve())
    failed_root = str(Path(args.failed_dir).resolve())
    queue_marker = str(Path(args.queue_dir).resolve() / f"{series_uid}.submitted")

    sbatch_args = [
        args.sbatch_script,
        staging_series,
        out_series,
        queue_marker,
        archive_root,
        failed_root,
    ]

    if args.mode == "dry-run":
        return ["echo", "[DRY-RUN] would submit:", "sbatch"] + sbatch_args

    if args.mode == "local":
        return ["sbatch"] + sbatch_args

    # ssh mode
    remote_cmd = " ".join(shlex.quote(a) for a in ["sbatch"] + sbatch_args)
    return ["ssh", args.ssh_host, remote_cmd]


def _submit_one(args, series_uid: str, claimed_marker: Path) -> bool:
    """Run the submit command. Returns True if accepted."""
    cmd = _build_submit_cmd(args, series_uid)
    print(f"  Submitting: {' '.join(shlex.quote(c) for c in cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        print(f"  !! Submission timed out")
        return False
    except Exception as e:
        print(f"  !! Submission error: {e}")
        return False

    if result.returncode != 0:
        print(f"  !! sbatch rejected (rc={result.returncode})")
        print(f"     stderr: {result.stderr.strip()}")
        return False

    out = result.stdout.strip()
    if out:
        print(f"  {out}")
    return True


def _mark_submitted(claimed: Path) -> None:
    claimed.rename(claimed.with_suffix(".submitted"))


def _mark_submit_failed(claimed: Path) -> None:
    failed = claimed.with_suffix(f".submit-failed_{_timestamp()}")
    claimed.rename(failed)
    print(f"  Marked {failed.name}")


def main():
    parser = argparse.ArgumentParser(description="DICOM sbatch submitter.")
    parser.add_argument("--queue-dir", required=True)
    parser.add_argument("--staging-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--archive-dir", required=True)
    parser.add_argument("--failed-dir", required=True)
    parser.add_argument("--sbatch-script", required=True,
                        help="Absolute path to the sbatch script on the HPC side.")
    parser.add_argument("--mode", choices=["dry-run", "local", "ssh"],
                        default="dry-run",
                        help="dry-run = echo the sbatch command. "
                             "local = run sbatch on this host. "
                             "ssh = run sbatch on a remote login node.")
    parser.add_argument("--ssh-host",
                        help="Required for --mode=ssh. e.g., user@login.hpc.example.org")
    parser.add_argument("--poll-interval", type=float, default=3.0)
    args = parser.parse_args()

    if args.mode == "ssh" and not args.ssh_host:
        parser.error("--ssh-host is required when --mode=ssh")

    for d in (args.queue_dir, args.staging_dir, args.out_dir,
              args.archive_dir, args.failed_dir):
        os.makedirs(d, exist_ok=True)

    queue = Path(args.queue_dir).resolve()
    print(f"Submitter polling {queue} every {args.poll_interval}s (mode={args.mode})")

    while True:
        markers = sorted(queue.glob("*.ready"))
        if not markers:
            time.sleep(args.poll_interval)
            continue

        for marker in markers:
            claimed = _claim(marker)
            if claimed is None:
                continue

            series_uid = marker.stem  # filename minus .ready
            print(f"Claimed {series_uid}")

            ok = _submit_one(args, series_uid, claimed)
            if ok:
                _mark_submitted(claimed)
                print(f"  -> submitted")
            else:
                _mark_submit_failed(claimed)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSubmitter stopped.")
