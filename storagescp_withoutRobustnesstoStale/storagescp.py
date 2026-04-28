"""DICOM Storage SCP — study-level grouping.

Receives DICOM C-STORE requests and groups arriving series by
(MRN, StudyDate). Each "study" is one unit of work for HPC.

Per association:

  1. handle_store: saves each file to in/<seriesUID>/<sopUID>.
  2. handle_released: for each newly-arrived series, reads one DICOM
     to extract MRN + StudyDate, then moves the series into
        study_pending/<MRN>/<DOS>/<seriesUID>/
     If this is the FIRST series for a given (MRN,DOS), records the
     arrival timestamp on .first_arrival.

A background watcher thread runs every 60 seconds. For each
study_pending/<MRN>/<DOS>/ whose .first_arrival is older than 20
minutes, the watcher promotes it: moves the whole study tree to
staging/<MRN>/<DOS>/ and writes queue/<MRN>_<DOS>.ready.

Late arrivals (a new series for an MRN+DOS that has already been
promoted, submitted, or completed) are routed to quarantine/.

Nothing is ever deleted. All transitions are mv.
"""

import argparse
import datetime as dt
import os
import shutil
import threading
import time
from pathlib import Path

import pydicom
from pynetdicom import (
    AE, debug_logger, evt,
    AllStoragePresentationContexts, ALL_TRANSFER_SYNTAXES,
)


# --- helpers ----------------------------------------------------------------

def ts():
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)


def safe_id(s):
    """Make a string safe for filesystem use. MRN and StudyDate are
    almost always already safe, but be defensive."""
    return "".join(c if c.isalnum() or c in "-." else "_" for c in str(s))


def read_study_keys(series_dir):
    """Read one DICOM file from series_dir to extract (MRN, StudyDate).
    Returns (mrn, dos) or (None, None) if unreadable / missing keys."""
    try:
        files = list(series_dir.iterdir())
        if not files:
            return None, None
        ds = pydicom.dcmread(str(files[0]), stop_before_pixels=True)
        mrn = str(getattr(ds, "PatientID", "")).strip()
        dos = str(getattr(ds, "StudyDate", "")).strip()
        if not mrn or not dos:
            return None, None
        return safe_id(mrn), safe_id(dos)
    except Exception as e:
        log(f"  !! Could not read {series_dir}: {e}")
        return None, None


# --- C-STORE handler --------------------------------------------------------

def handle_store(event, incomingDir):
    """Save the file to in/<seriesUID>/<sopUID>. Track UID on association."""
    ds = event.dataset
    ds.file_meta = event.file_meta

    series_uid = str(ds.SeriesInstanceUID)
    sop_uid = str(ds.SOPInstanceUID)

    series_dir = os.path.join(incomingDir, series_uid)
    os.makedirs(series_dir, exist_ok=True)

    ds.save_as(os.path.join(series_dir, sop_uid), enforce_file_format=True)

    assoc = event.assoc
    if not hasattr(assoc, "_received_series"):
        assoc._received_series = set()
    assoc._received_series.add(series_uid)

    return 0x0000


# --- association release handler --------------------------------------------

def _study_already_in_flight(mrn, dos, dirs):
    """Has this study already been promoted past pending? Returns True
    if staging/, queue/<mrn>_<dos>.{submitting,submitted,done_*,failed_*},
    archive/<mrn>_<dos>_*, or failed/<mrn>_<dos>_* exists."""
    flat = f"{mrn}_{dos}"

    # In staging?
    if (dirs["staging"] / mrn / dos).exists():
        return True

    # Has any non-.ready marker for this study?
    queue = dirs["queue"]
    for pat in (f"{flat}.submitting", f"{flat}.submitted",
                f"{flat}.done_*", f"{flat}.failed_*",
                f"{flat}.submit-failed_*"):
        if list(queue.glob(pat)):
            return True

    # Archived already?
    if list(dirs["archive"].glob(f"{flat}_*")):
        return True
    if list(dirs["failed"].glob(f"{flat}_*")):
        return True

    return False


def _quarantine(series_uid, src, dirs, reason):
    """Move a series to quarantine with a reason tag."""
    target = dirs["quarantine"] / f"{reason}_{series_uid}_{ts()}"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(target))
    log(f"  Quarantined {series_uid} -> {target.name} (reason: {reason})")


def _move_to_pending(mrn, dos, series_uid, src, dirs):
    """Move in/<seriesUID> to study_pending/<mrn>/<dos>/<seriesUID>.
    Set .first_arrival on first series of this (mrn,dos)."""
    study_dir = dirs["study_pending"] / mrn / dos
    is_new = not study_dir.exists()
    study_dir.mkdir(parents=True, exist_ok=True)

    target = study_dir / series_uid
    if target.exists():
        # Same series re-sent across associations. Merge: copy any new
        # files in, leave existing ones. Safe because SOP UIDs are unique.
        for f in src.iterdir():
            tgt = target / f.name
            if not tgt.exists():
                shutil.move(str(f), str(tgt))
        # src may be empty now; remove its directory
        try:
            src.rmdir()
        except OSError:
            pass
        log(f"  Merged {series_uid} into existing {mrn}/{dos}")
    else:
        shutil.move(str(src), str(target))
        log(f"  Staged {series_uid} -> study_pending/{mrn}/{dos}/")

    if is_new:
        (study_dir / ".first_arrival").write_text(ts() + "\n")
        log(f"  New study: {mrn}/{dos} (first arrival recorded)")


def handle_released(event, dirs):
    """Association released: route each newly-arrived series."""
    assoc = event.assoc
    received = getattr(assoc, "_received_series", set())
    log(f"Association with {assoc.requestor.ae_title} released. "
        f"Series received: {len(received)}")

    for series_uid in received:
        src = dirs["in"] / series_uid
        if not src.exists():
            log(f"  !! Source missing: {src}")
            continue

        mrn, dos = read_study_keys(src)
        if not mrn or not dos:
            _quarantine(series_uid, src, dirs, "no_mrn_or_dos")
            continue

        if _study_already_in_flight(mrn, dos, dirs):
            log(f"  Late arrival for {mrn}/{dos}")
            _quarantine(series_uid, src, dirs, f"late_{mrn}_{dos}")
            continue

        try:
            _move_to_pending(mrn, dos, series_uid, src, dirs)
        except Exception as e:
            log(f"  !! Move failed for {series_uid}: {e}")

    return 0x0000


# --- watcher thread (absolute timeout) --------------------------------------

def _read_first_arrival(study_dir):
    """Return datetime of first arrival, or None if missing/unreadable."""
    f = study_dir / ".first_arrival"
    if not f.exists():
        return None
    try:
        s = f.read_text().strip()
        return dt.datetime.strptime(s, "%Y-%m-%dT%H-%M-%SZ")
    except Exception:
        return None


def _promote_study(mrn, dos, dirs):
    """Move study_pending/<mrn>/<dos>/ -> staging/<mrn>/<dos>/, write
    queue/<mrn>_<dos>.ready."""
    src = dirs["study_pending"] / mrn / dos
    dst = dirs["staging"] / mrn / dos
    dst.parent.mkdir(parents=True, exist_ok=True)

    # List series before moving for the log
    series_uids = sorted(p.name for p in src.iterdir()
                         if p.is_dir())

    shutil.move(str(src), str(dst))

    flat = f"{mrn}_{dos}"
    marker = dirs["queue"] / f"{flat}.ready"
    marker.touch()

    log(f"Study {mrn}/{dos} promoted after timeout. "
        f"Series: {len(series_uids)}")
    for sid in series_uids:
        log(f"    {sid}")
    log(f"  marker: queue/{flat}.ready")

    # Try to remove the now-empty mrn/ parent if no other studies pending
    try:
        (dirs["study_pending"] / mrn).rmdir()
    except OSError:
        pass


def watcher_loop(dirs, timeout_minutes, poll_seconds, stop_event):
    """Run forever. Promote studies whose first_arrival is older than
    timeout_minutes."""
    log(f"Watcher started: timeout={timeout_minutes} min, poll={poll_seconds}s")
    while not stop_event.is_set():
        now = dt.datetime.utcnow()
        cutoff = now - dt.timedelta(minutes=timeout_minutes)

        try:
            mrn_dirs = [p for p in dirs["study_pending"].iterdir() if p.is_dir()]
        except FileNotFoundError:
            mrn_dirs = []

        for mrn_dir in mrn_dirs:
            try:
                dos_dirs = [p for p in mrn_dir.iterdir() if p.is_dir()]
            except FileNotFoundError:
                continue
            for dos_dir in dos_dirs:
                first = _read_first_arrival(dos_dir)
                if first is None:
                    continue
                if first < cutoff:
                    try:
                        _promote_study(mrn_dir.name, dos_dir.name, dirs)
                    except Exception as e:
                        log(f"!! Promotion failed for {mrn_dir.name}/"
                            f"{dos_dir.name}: {e}")

        stop_event.wait(timeout=poll_seconds)


# --- startup sweep ----------------------------------------------------------

def sweep_orphans(in_dir, orphaned_dir):
    """On startup, any series left in in/ from a prior crashed run gets
    moved to orphaned/."""
    in_path = Path(in_dir)
    if not in_path.exists():
        return
    moved = 0
    for entry in in_path.iterdir():
        if entry.is_dir():
            target = Path(orphaned_dir) / f"{entry.name}_{ts()}"
            shutil.move(str(entry), str(target))
            moved += 1
    if moved:
        log(f"Swept {moved} orphaned series from in/ to orphaned/")


# --- main -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DICOM Storage SCP (study-level grouping).")
    parser.add_argument("--incoming-dir", required=True)
    parser.add_argument("--study-pending-dir", required=True,
                        help="Per-study assembly area. Studies wait here for "
                             "the timeout before being promoted.")
    parser.add_argument("--staging-dir", required=True,
                        help="Studies promoted here once timeout fires; "
                             "submitter picks up from queue/<MRN>_<DOS>.ready.")
    parser.add_argument("--queue-dir", required=True)
    parser.add_argument("--orphaned-dir", required=True)
    parser.add_argument("--quarantine-dir", required=True,
                        help="Late arrivals and unreadable DICOMs go here.")
    parser.add_argument("--archive-dir", required=True,
                        help="Read-only here; used to detect already-completed studies.")
    parser.add_argument("--failed-dir", required=True,
                        help="Read-only here; used to detect already-failed studies.")
    parser.add_argument("--timeout-minutes", type=float, default=20.0,
                        help="Absolute timeout from first arrival of a study.")
    parser.add_argument("--watcher-poll-seconds", type=float, default=60.0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11112)
    parser.add_argument("--ae-title", default="ANY-SCP")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    # Group-writable umask in case the SCP also touches shared paths.
    os.umask(0o002)

    dirs = {
        "in":             Path(args.incoming_dir).resolve(),
        "study_pending":  Path(args.study_pending_dir).resolve(),
        "staging":        Path(args.staging_dir).resolve(),
        "queue":          Path(args.queue_dir).resolve(),
        "orphaned":       Path(args.orphaned_dir).resolve(),
        "quarantine":     Path(args.quarantine_dir).resolve(),
        "archive":        Path(args.archive_dir).resolve(),
        "failed":         Path(args.failed_dir).resolve(),
    }

    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    # in/ and study_pending/ must share a filesystem (atomic rename).
    if dirs["in"].stat().st_dev != dirs["study_pending"].stat().st_dev:
        log("!! WARNING: incoming-dir and study-pending-dir on different FS. "
            "Handoff will be a slow copy.")

    sweep_orphans(dirs["in"], dirs["orphaned"])

    if args.debug:
        debug_logger()

    handlers = [
        (evt.EVT_C_STORE, handle_store, [str(dirs["in"])]),
        (evt.EVT_RELEASED, handle_released, [dirs]),
    ]

    ae = AE(ae_title=args.ae_title)
    for cx in AllStoragePresentationContexts:
        ae.add_supported_context(cx.abstract_syntax, ALL_TRANSFER_SYNTAXES)

    # Background watcher
    stop_event = threading.Event()
    watcher = threading.Thread(
        target=watcher_loop,
        args=(dirs, args.timeout_minutes, args.watcher_poll_seconds, stop_event),
        daemon=True,
    )
    watcher.start()

    log(f"SCP listening on {args.host}:{args.port} (AE title: {args.ae_title})")
    log(f"  incoming:       {dirs['in']}")
    log(f"  study_pending:  {dirs['study_pending']}")
    log(f"  staging:        {dirs['staging']}")
    log(f"  queue:          {dirs['queue']}")
    log(f"  quarantine:     {dirs['quarantine']}")
    log(f"  timeout:        {args.timeout_minutes} min absolute")

    try:
        ae.start_server((args.host, args.port), evt_handlers=handlers)
    finally:
        stop_event.set()


if __name__ == "__main__":
    main()
