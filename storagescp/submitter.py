"""DICOM processing submitter — push/submit/poll/pull, study-keyed.

Each study is identified by a key <MRN>_<DOS>_<8-char studyuid hash>.
Filesystem layout for staging is nested:
  staging/<MRN>/<DOS>/<studyuid_safe>/<seriesUID>/<sopUID>...

Each cycle the submitter does three things:

  1. Recovery: any .submitting markers older than --recovery-stale-min get
     reset to .ready (transient submitter death; retry).
     Any .submitted markers whose Slurm job is no longer alive (per
     `sacct`) and whose scratch has no .done/.failed get marked .failed
     so we can pull-back/archive what's there.

  2. Launch new jobs: for each .ready -> rsync to scratch -> sbatch.
     Marker transitions: .ready -> .submitting -> .submitted (or
     .submit-failed_<ts>).

  3. Poll completions: for each .submitted, check scratch/.done or
     .failed. Pull back, archive locally + scratch, rename marker.

Requires `sacct` reachable via SSH for the death-check.
"""

import argparse
import datetime as dt
import grp
import os
import re
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


def split_study_key(skey):
    """<MRN>_<DOS>_<8hex> -> (mrn, dos, studyuid_hash). The 8-hex hash is
    always last; DOS is always 8 digits before that. MRN is everything
    earlier."""
    parts = skey.rsplit("_", 2)
    if len(parts) != 3:
        return skey, "", ""
    mrn, dos, sh = parts
    return mrn, dos, sh


def find_staging_dir(staging_root, skey):
    """Given a study key, find staging/<mrn>/<dos>/<studyuid_safe>/.
    The studyuid_safe directory name is whatever the SCP wrote — we
    search by hash match."""
    import hashlib
    mrn, dos, sh = split_study_key(skey)
    base = Path(staging_root) / mrn / dos
    if not base.exists():
        return None
    # Match the one whose hash equals sh
    for child in base.iterdir():
        if child.is_dir():
            actual_hash = hashlib.sha1(child.name.encode()).hexdigest()[:8]
            # The SCP stores studyuid sanitized by safe_id; we need to undo
            # nothing — we hash the same dir name back.
            if actual_hash == sh:
                return child
    # Fallback: if there's only one, take it
    children = [c for c in base.iterdir() if c.is_dir()]
    if len(children) == 1:
        return children[0]
    return None


# ---------------------------------------------------------------------------
# Permissions helpers
# ---------------------------------------------------------------------------

_shared_gid = None


def resolve_shared_gid(group_name):
    global _shared_gid
    if _shared_gid is None and group_name:
        try:
            _shared_gid = grp.getgrnam(group_name).gr_gid
            log(f"Shared group: {group_name} (gid {_shared_gid})")
        except KeyError:
            log(f"!! Group '{group_name}' not found")
            _shared_gid = -1
    return _shared_gid if _shared_gid and _shared_gid > 0 else None


def make_transit_dir(path, group_name=None):
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
# Recovery (run once at startup, then periodically)
# ---------------------------------------------------------------------------

def _marker_age_minutes(marker):
    try:
        mtime = dt.datetime.utcfromtimestamp(marker.stat().st_mtime)
        return (dt.datetime.utcnow() - mtime).total_seconds() / 60.0
    except FileNotFoundError:
        return None


def recover_submitting(args):
    """Reset stale .submitting markers back to .ready for retry."""
    queue = Path(args.queue_dir)
    threshold = args.recovery_stale_min
    for marker in sorted(queue.glob("*.submitting")):
        age = _marker_age_minutes(marker)
        if age is None or age < threshold:
            continue
        skey = marker.stem
        new = marker.with_suffix(".ready")
        try:
            marker.rename(new)
            log(f"RECOVERY: stale .submitting reset to .ready: {skey} (age {age:.1f}m)")
        except OSError as e:
            log(f"!! Could not reset {skey}: {e}")


def slurm_states_for_jobs(args, study_keys):
    """Query Slurm via SSH for the latest state of each study's most recent
    job. Returns {study_key: state_string_or_None}.

    Strategy: for each .submitted marker we don't track jobid persistently;
    instead we use sacct with --name to find jobs by job-name-pattern. We
    set the job name in process_series.sh to include the study_key prefix
    so we can correlate. The simplest approach: query all recent jobs by
    user, parse JobName.
    """
    if not study_keys:
        return {}

    # Pull recent jobs from sacct (last 7 days, oldest reasonable window).
    cmd = ["ssh", args.ssh_host,
           "sacct -u $USER -X -P -n --starttime now-7days "
           "--format=JobID,JobName,State"]
    rc, stdout, stderr = run(cmd, timeout=30)
    if rc != 0:
        log(f"  !! sacct query failed: {stderr.strip()}")
        return {}

    # Map JobName -> latest State (sacct may have multiple rows per job).
    name_to_state = {}
    for line in stdout.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        _, job_name, state = parts[0], parts[1], parts[2]
        # Take primary state (strip "CANCELLED by ..." suffix).
        primary = state.split()[0] if state else ""
        name_to_state[job_name] = primary

    # Match each study key to a job name.
    # Job name in process_series.sh is set as: dicom-<skey>
    # We use this to correlate.
    result = {}
    for skey in study_keys:
        expected_name = f"dicom-{skey}"
        state = name_to_state.get(expected_name)
        result[skey] = state
    return result


# Slurm states considered "dead without writing a marker" — treat as failed.
DEAD_STATES = {"FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL",
               "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY", "PREEMPTED"}
ALIVE_STATES = {"PENDING", "RUNNING", "REQUEUED", "RESIZING", "SUSPENDED",
                "CONFIGURING", "COMPLETING"}


def detect_dead_jobs(args):
    """Check .submitted markers against Slurm. If a job is in a dead state
    and scratch has no .done/.failed, write .failed ourselves so the normal
    pull-back path runs."""
    queue = Path(args.queue_dir)
    submitted = list(queue.glob("*.submitted"))
    if not submitted:
        return

    skeys = [m.stem for m in submitted]
    states = slurm_states_for_jobs(args, skeys)

    for marker in submitted:
        skey = marker.stem
        ws_scratch = Path(args.scratch_dir) / skey
        if (ws_scratch / ".done").exists() or (ws_scratch / ".failed").exists():
            continue  # normal completion path will handle it

        state = states.get(skey)
        if state is None:
            continue  # don't know — could be too new for sacct
        if state in ALIVE_STATES:
            continue  # still going
        if state in DEAD_STATES:
            log(f"DEAD JOB DETECTED: {skey} (Slurm state: {state}). "
                f"Writing .failed to scratch.")
            try:
                make_transit_dir(ws_scratch, args.shared_group)
                (ws_scratch / ".failed").touch()
            except OSError as e:
                log(f"  !! Could not write .failed: {e}")


# ---------------------------------------------------------------------------
# Launch new jobs
# ---------------------------------------------------------------------------

def launch_job(args, skey):
    local_staging = find_staging_dir(args.staging_dir, skey)
    if local_staging is None:
        log(f"  !! Local staging dir not found for {skey}")
        return False

    ws_scratch = Path(args.scratch_dir) / skey
    ws_scratch_input = ws_scratch / "input"
    ws_scratch_output = ws_scratch / "output"
    hpc_scratch = ws_to_hpc(str(ws_scratch), args.workstation_prefix, args.hpc_prefix)

    make_transit_dir(ws_scratch, args.shared_group)
    make_transit_dir(ws_scratch_input, args.shared_group)
    make_transit_dir(ws_scratch_output, args.shared_group)

    log(f"  rsync {local_staging} -> scratch/{skey}/input/")
    rc, _, stderr = run([
        "rsync", "-a", "--no-owner", "--no-group",
        "--chmod=Dg+rwxs,Fg+rw",
        f"{local_staging}/", f"{ws_scratch_input}/",
    ], timeout=3600)
    if rc != 0:
        log(f"  !! rsync push failed: {stderr.strip()}")
        return False

    # Job name embeds the study key for sacct correlation later.
    remote_cmd = (f"sbatch --job-name=dicom-{shlex.quote(skey)} "
                  f"{shlex.quote(args.sbatch_script)} "
                  f"{shlex.quote(hpc_scratch)}")
    log(f"  sbatch on HPC: {hpc_scratch}")
    rc, stdout, stderr = run(["ssh", args.ssh_host, remote_cmd], timeout=120)
    if rc != 0:
        log(f"  !! sbatch rejected: {stderr.strip()}")
        return False

    for line in stdout.splitlines():
        if line.startswith("Submitted batch job"):
            log(f"  Slurm job {line.split()[-1]} submitted (name=dicom-{skey})")
    return True


def claim_and_launch(args, ready_marker):
    submitting = ready_marker.with_suffix(".submitting")
    try:
        ready_marker.rename(submitting)
    except FileNotFoundError:
        return

    skey = ready_marker.stem
    log(f"Launching study {skey}")

    ok = launch_job(args, skey)
    if ok:
        submitting.rename(submitting.with_suffix(".submitted"))
        log(f"  -> submitted")
    else:
        failed = submitting.with_name(f"{skey}.submit-failed_{ts()}")
        submitting.rename(failed)
        log(f"  -> submit-failed (see {failed.name})")


# ---------------------------------------------------------------------------
# Poll and pull back
# ---------------------------------------------------------------------------

def pull_and_archive(args, skey, success):
    local_staging = find_staging_dir(args.staging_dir, skey)
    ws_scratch = Path(args.scratch_dir) / skey
    ws_scratch_output = ws_scratch / "output"

    timestamp = ts()
    if success:
        archive_target = Path(args.archive_dir) / f"{skey}_{timestamp}"
        scratch_archive = Path(args.scratch_archive_dir) / f"{skey}_{timestamp}"
        marker_suffix = f".done_{timestamp}"
        # output goes to out/<MRN>/<DOS>/<studyuid_safe>/
        if local_staging is not None:
            local_out = Path(args.out_dir) / local_staging.parent.parent.name / \
                        local_staging.parent.name / local_staging.name
        else:
            local_out = Path(args.out_dir) / skey
    else:
        archive_target = Path(args.failed_dir) / f"{skey}_{timestamp}"
        scratch_archive = Path(args.scratch_failed_dir) / f"{skey}_{timestamp}"
        marker_suffix = f".failed_{timestamp}"
        local_out = None

    if success and ws_scratch_output.exists():
        local_out.mkdir(parents=True, exist_ok=True)
        log(f"  rsync output -> {local_out}")
        rc, _, stderr = run([
            "rsync", "-a", "--no-owner", "--no-group",
            f"{ws_scratch_output}/", f"{local_out}/",
        ], timeout=3600)
        if rc != 0:
            log(f"  !! rsync pull failed: {stderr.strip()}")
            return

    if local_staging is not None and local_staging.exists():
        archive_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(local_staging), str(archive_target))
        log(f"  local staging -> {archive_target}")
        # Try to remove now-empty <DOS>/ and <MRN>/ parents.
        for parent in (local_staging.parent, local_staging.parent.parent):
            try:
                parent.rmdir()
            except OSError:
                pass

    if ws_scratch.exists():
        make_transit_dir(scratch_archive.parent, args.shared_group)
        shutil.move(str(ws_scratch), str(scratch_archive))
        log(f"  scratch -> {scratch_archive}")

    submitted_marker = Path(args.queue_dir) / f"{skey}.submitted"
    if submitted_marker.exists():
        submitted_marker.rename(submitted_marker.with_name(skey + marker_suffix))


def check_completions(args):
    queue = Path(args.queue_dir)
    for marker in sorted(queue.glob("*.submitted")):
        skey = marker.stem
        ws_scratch = Path(args.scratch_dir) / skey
        done = ws_scratch / ".done"
        failed = ws_scratch / ".failed"

        if done.exists():
            log(f"Completion detected: {skey} (success)")
            pull_and_archive(args, skey, success=True)
        elif failed.exists():
            log(f"Completion detected: {skey} (failed)")
            pull_and_archive(args, skey, success=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="DICOM async submitter (study-keyed).")

    p.add_argument("--staging-dir", required=True,
                   help="Workstation staging/<MRN>/<DOS>/<studyuid_safe>/")
    p.add_argument("--queue-dir", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--archive-dir", required=True)
    p.add_argument("--failed-dir", required=True)

    p.add_argument("--scratch-dir", required=True)
    p.add_argument("--scratch-archive-dir", required=True)
    p.add_argument("--scratch-failed-dir", required=True)

    p.add_argument("--ssh-host", required=True)
    p.add_argument("--sbatch-script", required=True)
    p.add_argument("--workstation-prefix", required=True)
    p.add_argument("--hpc-prefix", required=True)
    p.add_argument("--shared-group", default="")

    p.add_argument("--poll-interval", type=float, default=30.0)
    p.add_argument("--recovery-stale-min", type=float, default=10.0,
                   help="Stale .submitting markers older than this are reset to .ready.")
    p.add_argument("--slurm-check-interval", type=float, default=300.0,
                   help="How often to check Slurm for dead jobs (seconds).")
    args = p.parse_args()

    os.umask(0o002)

    for d in (args.staging_dir, args.queue_dir, args.out_dir,
              args.archive_dir, args.failed_dir):
        os.makedirs(d, exist_ok=True)
    for d in (args.scratch_dir, args.scratch_archive_dir, args.scratch_failed_dir):
        make_transit_dir(d, args.shared_group)

    log(f"Submitter started.")
    log(f"  poll:               {args.poll_interval}s")
    log(f"  recovery threshold: {args.recovery_stale_min} min")
    log(f"  slurm check every:  {args.slurm_check_interval}s")

    # Startup recovery sweep.
    log("Startup recovery sweep...")
    recover_submitting(args)
    detect_dead_jobs(args)

    last_slurm_check = time.time()

    while True:
        # 1. Periodic recovery
        recover_submitting(args)

        # 2. Periodic Slurm check (less often — sacct can be slow)
        now = time.time()
        if now - last_slurm_check >= args.slurm_check_interval:
            detect_dead_jobs(args)
            last_slurm_check = now

        # 3. Launch any .ready
        queue = Path(args.queue_dir)
        for ready in sorted(queue.glob("*.ready")):
            claim_and_launch(args, ready)

        # 4. Pull back any completed
        check_completions(args)

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Stopped.")
