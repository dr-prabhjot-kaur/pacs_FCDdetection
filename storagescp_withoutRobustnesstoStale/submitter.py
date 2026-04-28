"""DICOM processing submitter — push/submit/poll/pull, study-level units.

Queue markers are <MRN>_<DOS>.ready / .submitting / .submitted / .done_<ts>
/ .failed_<ts>. Staging directories are nested as staging/<MRN>/<DOS>/.

Each cycle:

  1. For each queue/<MRN>_<DOS>.ready:
       - rsync staging/<MRN>/<DOS>/ -> /fileserver/scratch/<MRN>_<DOS>/input/
       - ssh HPC sbatch <script> <hpc-scratch>
       - rename .ready -> .submitted

  2. For each queue/<MRN>_<DOS>.submitted:
       - check /fileserver/scratch/<MRN>_<DOS>/.done or .failed
       - on success: rsync output back, archive locally + scratch
       - on failure: archive locally to failed/, scratch to scratch_failed/

Jobs run in parallel on HPC. Nothing is deleted; everything moves.
"""

import argparse
import datetime as dt
import grp
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path


def ts():
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd, timeout=None):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def ws_to_hpc(path, ws_prefix, hpc_prefix):
    if path.startswith(ws_prefix):
        return hpc_prefix + path[len(ws_prefix):]
    return path


def split_study_id(study_id):
    """<MRN>_<DOS> -> (mrn, dos). MRN can contain underscores; we split on
    the LAST underscore so DOS (always digits) takes the right side."""
    if "_" not in study_id:
        return study_id, ""
    mrn, dos = study_id.rsplit("_", 1)
    return mrn, dos


# ---------------------------------------------------------------------------
# Shared-storage helpers (group + setgid for HPC writability)
# ---------------------------------------------------------------------------

_shared_gid = None


def resolve_shared_gid(group_name):
    global _shared_gid
    if _shared_gid is None and group_name:
        try:
            _shared_gid = grp.getgrnam(group_name).gr_gid
            log(f"Shared group: {group_name} (gid {_shared_gid})")
        except KeyError:
            log(f"!! Group '{group_name}' not found — skipping chgrp")
            _shared_gid = -1
    return _shared_gid if _shared_gid and _shared_gid > 0 else None


def make_transit_dir(path, group_name=None):
    """Create dir on shared storage with mode 02775 + shared group."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(p, 0o2775)
    except PermissionError:
        pass
    gid = resolve_shared_gid(group_name)
    if gid is not None:
        try:
            os.chown(p, -1, gid)
        except PermissionError:
            pass


# ---------------------------------------------------------------------------
# Launch new jobs
# ---------------------------------------------------------------------------

def launch_job(args, study_id):
    """rsync study to scratch, submit sbatch."""
    mrn, dos = split_study_id(study_id)
    local_staging = Path(args.staging_dir) / mrn / dos
    ws_scratch = Path(args.scratch_dir) / study_id
    ws_scratch_input = ws_scratch / "input"
    ws_scratch_output = ws_scratch / "output"
    hpc_scratch = ws_to_hpc(str(ws_scratch), args.workstation_prefix, args.hpc_prefix)

    if not local_staging.is_dir():
        log(f"  !! Local staging missing: {local_staging}")
        return False

    make_transit_dir(ws_scratch, args.shared_group)
    make_transit_dir(ws_scratch_input, args.shared_group)
    make_transit_dir(ws_scratch_output, args.shared_group)

    log(f"  rsync staging/{mrn}/{dos}/ -> scratch/{study_id}/input/")
    rc, _, stderr = run([
        "rsync", "-a", "--no-owner", "--no-group",
        "--chmod=Dg+rwxs,Fg+rw",
        f"{local_staging}/", f"{ws_scratch_input}/",
    ], timeout=3600)
    if rc != 0:
        log(f"  !! rsync push failed: {stderr.strip()}")
        return False

    remote_cmd = f"sbatch {shlex.quote(args.sbatch_script)} {shlex.quote(hpc_scratch)}"
    log(f"  sbatch on HPC: {hpc_scratch}")
    rc, stdout, stderr = run(["ssh", args.ssh_host, remote_cmd], timeout=120)
    if rc != 0:
        log(f"  !! sbatch rejected: {stderr.strip()}")
        return False

    for line in stdout.splitlines():
        if line.startswith("Submitted batch job"):
            log(f"  Slurm job {line.split()[-1]} submitted")
    return True


def claim_and_launch(args, ready_marker):
    submitting = ready_marker.with_suffix(".submitting")
    try:
        ready_marker.rename(submitting)
    except FileNotFoundError:
        return

    study_id = ready_marker.stem
    log(f"Launching study {study_id}")

    ok = launch_job(args, study_id)
    if ok:
        submitting.rename(submitting.with_suffix(".submitted"))
        log(f"  -> submitted")
    else:
        failed = submitting.with_name(f"{study_id}.submit-failed_{ts()}")
        submitting.rename(failed)
        log(f"  -> submit-failed (see {failed.name})")


# ---------------------------------------------------------------------------
# Poll and pull back completions
# ---------------------------------------------------------------------------

def pull_and_archive(args, study_id, success):
    mrn, dos = split_study_id(study_id)
    local_staging = Path(args.staging_dir) / mrn / dos
    local_out = Path(args.out_dir) / mrn / dos
    ws_scratch = Path(args.scratch_dir) / study_id
    ws_scratch_output = ws_scratch / "output"

    timestamp = ts()
    if success:
        archive_target = Path(args.archive_dir) / f"{study_id}_{timestamp}"
        scratch_archive = Path(args.scratch_archive_dir) / f"{study_id}_{timestamp}"
        marker_suffix = f".done_{timestamp}"
    else:
        archive_target = Path(args.failed_dir) / f"{study_id}_{timestamp}"
        scratch_archive = Path(args.scratch_failed_dir) / f"{study_id}_{timestamp}"
        marker_suffix = f".failed_{timestamp}"

    if success and ws_scratch_output.exists():
        local_out.mkdir(parents=True, exist_ok=True)
        log(f"  rsync output -> local out/{mrn}/{dos}/")
        rc, _, stderr = run([
            "rsync", "-a", "--no-owner", "--no-group",
            f"{ws_scratch_output}/", f"{local_out}/",
        ], timeout=3600)
        if rc != 0:
            log(f"  !! rsync pull failed: {stderr.strip()}")
            return

    if local_staging.exists():
        archive_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(local_staging), str(archive_target))
        log(f"  local staging -> {archive_target}")
        # Remove now-empty <mrn>/ parent if no other DOS pending
        try:
            local_staging.parent.rmdir()
        except OSError:
            pass

    if ws_scratch.exists():
        make_transit_dir(scratch_archive.parent, args.shared_group)
        shutil.move(str(ws_scratch), str(scratch_archive))
        log(f"  scratch -> {scratch_archive}")

    submitted_marker = Path(args.queue_dir) / f"{study_id}.submitted"
    if submitted_marker.exists():
        submitted_marker.rename(submitted_marker.with_name(study_id + marker_suffix))


def check_completions(args):
    queue = Path(args.queue_dir)
    for marker in sorted(queue.glob("*.submitted")):
        study_id = marker.stem
        ws_scratch = Path(args.scratch_dir) / study_id
        done = ws_scratch / ".done"
        failed = ws_scratch / ".failed"

        if done.exists():
            log(f"Completion detected: {study_id} (success)")
            pull_and_archive(args, study_id, success=True)
        elif failed.exists():
            log(f"Completion detected: {study_id} (failed)")
            pull_and_archive(args, study_id, success=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="DICOM async submitter (study-level).")

    # Local (workstation) dirs
    p.add_argument("--staging-dir", required=True,
                   help="Workstation staging/<MRN>/<DOS>/ tree.")
    p.add_argument("--queue-dir", required=True)
    p.add_argument("--out-dir", required=True,
                   help="Final outputs land in out/<MRN>/<DOS>/.")
    p.add_argument("--archive-dir", required=True)
    p.add_argument("--failed-dir", required=True)

    # Transit dirs on /fileserver/
    p.add_argument("--scratch-dir", required=True)
    p.add_argument("--scratch-archive-dir", required=True)
    p.add_argument("--scratch-failed-dir", required=True)

    # HPC
    p.add_argument("--ssh-host", required=True)
    p.add_argument("--sbatch-script", required=True)
    p.add_argument("--workstation-prefix", required=True)
    p.add_argument("--hpc-prefix", required=True)
    p.add_argument("--shared-group", default="")

    p.add_argument("--poll-interval", type=float, default=30.0)
    args = p.parse_args()

    os.umask(0o002)

    for d in (args.staging_dir, args.queue_dir, args.out_dir,
              args.archive_dir, args.failed_dir):
        os.makedirs(d, exist_ok=True)

    for d in (args.scratch_dir, args.scratch_archive_dir, args.scratch_failed_dir):
        make_transit_dir(d, args.shared_group)

    log(f"Submitter started. Polling every {args.poll_interval}s.")
    log(f"  queue:        {args.queue_dir}")
    log(f"  scratch (ws): {args.scratch_dir}")
    log(f"  ssh host:     {args.ssh_host}")

    while True:
        queue = Path(args.queue_dir)
        for ready in sorted(queue.glob("*.ready")):
            claim_and_launch(args, ready)
        check_completions(args)
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Stopped.")
