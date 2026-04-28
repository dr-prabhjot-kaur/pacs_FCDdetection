"""DICOM processing submitter — push/submit/poll/pull architecture.

The submitter is a long-running loop that does two things each cycle:

  1. Launch new jobs (for each queue/<uid>.ready):
       - rsync local staging/<uid>/ -> /fileserver/scratch/<uid>/input/
       - ssh HPC sbatch <script> <hpc-scratch-path>
       - rename marker .ready -> .submitted

  2. Poll in-flight jobs (for each queue/<uid>.submitted):
       - Check /fileserver/scratch/<uid>/.done or .failed
         (written by the sbatch script when processing ends).
       - On .done:
           rsync scratch/<uid>/output/ -> local out/<uid>/
           mv local staging/<uid>     -> local archive/<uid>_<ts>/
           mv /fileserver/scratch/<uid>/ -> /fileserver/scratch_archive/<uid>_<ts>/
           rename marker .submitted -> .done_<ts>
       - On .failed:
           mv local staging/<uid>     -> local failed/<uid>_<ts>/
           mv /fileserver/scratch/<uid>/ -> /fileserver/scratch_failed/<uid>_<ts>/
           rename marker .submitted -> .failed_<ts>

Jobs run in parallel on HPC. Nothing is ever deleted — every cleanup
is a move.
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
    """Run a subprocess; return (rc, stdout, stderr)."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def ws_to_hpc(path, ws_prefix, hpc_prefix):
    """Rewrite a /fileserver/... path to /lab-share/... for the HPC side."""
    if path.startswith(ws_prefix):
        return hpc_prefix + path[len(ws_prefix):]
    return path


# ---------------------------------------------------------------------------
# Shared-storage helpers
# ---------------------------------------------------------------------------
#
# The workstation and HPC compute nodes see the same user (ch239656) as
# different numeric UIDs but share a group. So anything we create on
# /fileserver/ that HPC needs to write to must be:
#   - owned by the shared group
#   - group-writable
#   - have setgid bit set (so children inherit the group)
# Mode 02775 is drwxrwsr-x, i.e. group-writable + setgid.

_shared_gid = None


def resolve_shared_gid(group_name):
    """Look up and cache the GID for the shared group name."""
    global _shared_gid
    if _shared_gid is None and group_name:
        try:
            _shared_gid = grp.getgrnam(group_name).gr_gid
            log(f"Shared group: {group_name} (gid {_shared_gid})")
        except KeyError:
            log(f"!! Group '{group_name}' not found — skipping chgrp")
            _shared_gid = -1  # sentinel: tried and failed
    return _shared_gid if _shared_gid and _shared_gid > 0 else None


def make_transit_dir(path, group_name=None):
    """Create a directory on shared storage with proper group + permissions.

    Sets mode 02775 (drwxrwsr-x) so: owner+group can write, others read,
    and new children inherit the group via setgid.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(p, 0o2775)
    except PermissionError:
        pass  # best effort — if we don't own it, leave it alone
    gid = resolve_shared_gid(group_name)
    if gid is not None:
        try:
            os.chown(p, -1, gid)
        except PermissionError:
            pass


# ---------------------------------------------------------------------------
# Launch new jobs
# ---------------------------------------------------------------------------

def launch_job(args, series_uid):
    """rsync data to scratch, submit sbatch. Returns True if submission OK."""
    local_staging = Path(args.staging_dir) / series_uid
    ws_scratch = Path(args.scratch_dir) / series_uid
    ws_scratch_input = ws_scratch / "input"
    ws_scratch_output = ws_scratch / "output"
    hpc_scratch = ws_to_hpc(str(ws_scratch), args.workstation_prefix, args.hpc_prefix)

    if not local_staging.is_dir():
        log(f"  !! Local staging missing: {local_staging}")
        return False

    # Create scratch structure with group-writable permissions so HPC
    # compute nodes can write into them.
    make_transit_dir(ws_scratch, args.shared_group)
    make_transit_dir(ws_scratch_input, args.shared_group)
    make_transit_dir(ws_scratch_output, args.shared_group)

    # rsync local -> fileserver scratch input (trailing slashes: copy contents).
    # --chmod forces group-writable perms on destination so HPC compute nodes
    # (different UID, same group via NFS) can write into these dirs.
    log(f"  rsync -> scratch/{series_uid}/input/")
    rc, _, stderr = run([
        "rsync", "-a", "--no-owner", "--no-group",
        "--chmod=Dg+rwxs,Fg+rw",
        f"{local_staging}/", f"{ws_scratch_input}/",
    ], timeout=3600)
    if rc != 0:
        log(f"  !! rsync push failed: {stderr.strip()}")
        return False

    # Submit via SSH — sbatch script takes one arg: HPC-side scratch root.
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
    """Atomically claim a .ready marker and launch the job."""
    submitting = ready_marker.with_suffix(".submitting")
    try:
        ready_marker.rename(submitting)
    except FileNotFoundError:
        return

    series_uid = ready_marker.stem
    log(f"Launching {series_uid}")

    ok = launch_job(args, series_uid)
    if ok:
        submitting.rename(submitting.with_suffix(".submitted"))
        log(f"  -> submitted")
    else:
        failed = submitting.with_name(f"{series_uid}.submit-failed_{ts()}")
        submitting.rename(failed)
        log(f"  -> submit-failed (see {failed.name})")


# ---------------------------------------------------------------------------
# Poll in-flight and pull back completions
# ---------------------------------------------------------------------------

def pull_and_archive(args, series_uid, success):
    """On completion: rsync results back, archive local & scratch copies."""
    local_staging = Path(args.staging_dir) / series_uid
    local_out = Path(args.out_dir) / series_uid
    ws_scratch = Path(args.scratch_dir) / series_uid
    ws_scratch_output = ws_scratch / "output"

    timestamp = ts()
    if success:
        archive_target = Path(args.archive_dir) / f"{series_uid}_{timestamp}"
        scratch_archive = Path(args.scratch_archive_dir) / f"{series_uid}_{timestamp}"
        marker_suffix = f".done_{timestamp}"
    else:
        archive_target = Path(args.failed_dir) / f"{series_uid}_{timestamp}"
        scratch_archive = Path(args.scratch_failed_dir) / f"{series_uid}_{timestamp}"
        marker_suffix = f".failed_{timestamp}"

    # Pull output back on success
    if success and ws_scratch_output.exists():
        local_out.mkdir(parents=True, exist_ok=True)
        log(f"  rsync output -> local out/{series_uid}/")
        rc, _, stderr = run([
            "rsync", "-a", "--no-owner", "--no-group",
            f"{ws_scratch_output}/", f"{local_out}/",
        ], timeout=3600)
        if rc != 0:
            log(f"  !! rsync pull failed: {stderr.strip()}")
            return  # leave markers as-is, next poll will retry

    # Archive local staging
    if local_staging.exists():
        archive_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(local_staging), str(archive_target))
        log(f"  local staging -> {archive_target}")

    # Preserve scratch (HPC-side copy, logs, done/failed marker)
    if ws_scratch.exists():
        make_transit_dir(scratch_archive.parent, args.shared_group)
        shutil.move(str(ws_scratch), str(scratch_archive))
        log(f"  scratch -> {scratch_archive}")

    # Rename queue marker
    submitted_marker = Path(args.queue_dir) / f"{series_uid}.submitted"
    if submitted_marker.exists():
        submitted_marker.rename(submitted_marker.with_name(series_uid + marker_suffix))


def check_completions(args):
    """Scan .submitted markers; pull back any that finished."""
    queue = Path(args.queue_dir)
    for marker in sorted(queue.glob("*.submitted")):
        series_uid = marker.stem
        ws_scratch = Path(args.scratch_dir) / series_uid
        done = ws_scratch / ".done"
        failed = ws_scratch / ".failed"

        if done.exists():
            log(f"Completion detected: {series_uid} (success)")
            pull_and_archive(args, series_uid, success=True)
        elif failed.exists():
            log(f"Completion detected: {series_uid} (failed)")
            pull_and_archive(args, series_uid, success=False)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="DICOM async submitter.")

    # Local (workstation) dirs
    p.add_argument("--staging-dir", required=True)
    p.add_argument("--queue-dir", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--archive-dir", required=True)
    p.add_argument("--failed-dir", required=True)

    # Transit dirs on /fileserver/
    p.add_argument("--scratch-dir", required=True,
                   help="/fileserver/.../scratch - per-job working dirs.")
    p.add_argument("--scratch-archive-dir", required=True,
                   help="/fileserver/.../scratch_archive - preserved after success.")
    p.add_argument("--scratch-failed-dir", required=True,
                   help="/fileserver/.../scratch_failed - preserved after failure.")

    # HPC
    p.add_argument("--ssh-host", required=True)
    p.add_argument("--sbatch-script", required=True,
                   help="HPC-side (/lab-share/...) path to process_series.sh")
    p.add_argument("--workstation-prefix", required=True,
                   help="e.g., /fileserver")
    p.add_argument("--hpc-prefix", required=True,
                   help="e.g., /lab-share")
    p.add_argument("--shared-group", default="",
                   help="Unix group shared by workstation and HPC accounts. "
                        "If set, transit directories created on /fileserver/ "
                        "get chgrp'd to this group + setgid + group-writable "
                        "so HPC compute nodes (different UID, same group) "
                        "can write to them.")

    p.add_argument("--poll-interval", type=float, default=30.0)
    args = p.parse_args()

    # Group-writable perms on anything we create (664 files, 775 dirs
    # after setgid). Combined with --shared-group, this guarantees HPC
    # can write into dirs we create, even if the filesystem setgid bit
    # gets lost somewhere.
    os.umask(0o002)

    # Local dirs: plain mkdir is fine (workstation-only).
    for d in (args.staging_dir, args.queue_dir, args.out_dir,
              args.archive_dir, args.failed_dir):
        os.makedirs(d, exist_ok=True)

    # Transit dirs on shared storage: must be group-writable + setgid.
    for d in (args.scratch_dir, args.scratch_archive_dir, args.scratch_failed_dir):
        make_transit_dir(d, args.shared_group)

    log(f"Submitter started. Polling every {args.poll_interval}s.")
    log(f"  queue:           {args.queue_dir}")
    log(f"  scratch (ws):    {args.scratch_dir}")
    log(f"  ssh host:        {args.ssh_host}")
    log(f"  sbatch script:   {args.sbatch_script}")

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
