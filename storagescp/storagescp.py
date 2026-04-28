"""DICOM Storage SCP — study-level grouping by (MRN, DOS, StudyInstanceUID).

Receives DICOM C-STORE requests and groups arriving series by
(PatientID, StudyDate, StudyInstanceUID). Each "study" is one unit of
work for HPC.

Per association:

  1. handle_store: saves each file to in/<seriesUID>/<sopUID>.
  2. handle_released or handle_aborted:
     For each newly-arrived series:
       - Read one DICOM to extract MRN, DOS, StudyInstanceUID.
       - Move in/<seriesUID> -> study_pending/<MRN>/<DOS>/<StudyUID>/<seriesUID>/.
       - If first arrival for this study, record .first_arrival timestamp.

Aborted associations are handled the same as releases — whatever made
it across is real (each C-STORE was acked), so we hand it off. This
prevents in/ from accumulating.

A background watcher thread (60s poll) promotes any study whose
.first_arrival is older than the configured timeout (default 20 min).
Promotion = move to staging/, write queue/<study_key>.ready.

Late arrivals -> quarantine.
Malformed DICOMs (no MRN/DOS/StudyUID) -> quarantine.

Study key (used for queue marker filenames):
  <MRN>_<DOS>_<short_studyuid_hash>

Filesystem layout uses the FULL StudyInstanceUID (browsable). Markers
use a hash because StudyUIDs are 64+ chars.

Nothing is deleted. All transitions are mv.
"""

import argparse
import datetime as dt
import hashlib
import os
import shutil
import threading
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
    """Make a string safe for filesystem use."""
    return "".join(c if c.isalnum() or c in "-." else "_" for c in str(s))


def study_uid_hash(study_uid):
    """Short stable hash of a StudyInstanceUID for use in marker filenames."""
    return hashlib.sha1(study_uid.encode()).hexdigest()[:8]


def study_key(mrn, dos, study_uid):
    """The flat identifier used in queue marker filenames."""
    return f"{mrn}_{dos}_{study_uid_hash(study_uid)}"


def read_study_keys(series_dir):
    """Return (mrn, dos, study_uid) or (None, None, None)."""
    try:
        files = list(series_dir.iterdir())
        if not files:
            return None, None, None
        ds = pydicom.dcmread(str(files[0]), stop_before_pixels=True)
        mrn = str(getattr(ds, "PatientID", "")).strip()
        dos = str(getattr(ds, "StudyDate", "")).strip()
        study_uid = str(getattr(ds, "StudyInstanceUID", "")).strip()
        if not mrn or not dos or not study_uid:
            return None, None, None
        return safe_id(mrn), safe_id(dos), study_uid  # raw study_uid for path; hash used in markers
    except Exception as e:
        log(f"  !! Could not read {series_dir}: {e}")
        return None, None, None


# --- C-STORE handler --------------------------------------------------------

def handle_store(event, incomingDir):
    """Save the file. Track UID on association object."""
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


# --- end-of-association handlers --------------------------------------------

def _study_already_in_flight(skey, dirs):
    """True if the study (by key) has already been promoted past pending."""
    queue = dirs["queue"]
    for pat in (f"{skey}.submitting", f"{skey}.submitted",
                f"{skey}.done_*", f"{skey}.failed_*",
                f"{skey}.submit-failed_*"):
        if list(queue.glob(pat)):
            return True
    if list(dirs["archive"].glob(f"{skey}_*")):
        return True
    if list(dirs["failed"].glob(f"{skey}_*")):
        return True
    return False


def _staging_path(mrn, dos, study_uid, dirs):
    return dirs["staging"] / mrn / dos / safe_id(study_uid)


def _pending_path(mrn, dos, study_uid, dirs):
    return dirs["study_pending"] / mrn / dos / safe_id(study_uid)


def _quarantine(series_uid, src, dirs, reason):
    target = dirs["quarantine"] / f"{reason}_{series_uid}_{ts()}"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(target))
    log(f"  Quarantined {series_uid} -> {target.name} (reason: {reason})")


def _handoff_one_series(series_uid, dirs):
    """Move in/<seriesUID> to the right place. Returns True if staged."""
    src = dirs["in"] / series_uid
    if not src.exists():
        log(f"  !! Source missing: {src}")
        return False

    mrn, dos, study_uid = read_study_keys(src)
    if not (mrn and dos and study_uid):
        _quarantine(series_uid, src, dirs, "no_mrn_dos_or_studyuid")
        return False

    skey = study_key(mrn, dos, study_uid)

    # Already past pending? Late arrival.
    if _staging_path(mrn, dos, study_uid, dirs).exists() or _study_already_in_flight(skey, dirs):
        log(f"  Late arrival for {mrn}/{dos}/{safe_id(study_uid)[:16]}...")
        _quarantine(series_uid, src, dirs, f"late_{skey}")
        return False

    pending_study = _pending_path(mrn, dos, study_uid, dirs)
    is_new_study = not pending_study.exists()
    pending_study.mkdir(parents=True, exist_ok=True)

    target = pending_study / series_uid
    if target.exists():
        # Same series re-sent; merge SOP files (no overwrite).
        for f in src.iterdir():
            tgt = target / f.name
            if not tgt.exists():
                shutil.move(str(f), str(tgt))
        try:
            src.rmdir()
        except OSError:
            pass
        log(f"  Merged {series_uid} into {mrn}/{dos}/{safe_id(study_uid)[:16]}...")
    else:
        shutil.move(str(src), str(target))
        log(f"  Staged {series_uid} -> study_pending/{mrn}/{dos}/{safe_id(study_uid)[:16]}.../")

    if is_new_study:
        (pending_study / ".first_arrival").write_text(ts() + "\n")
        (pending_study / ".study_uid").write_text(study_uid + "\n")
        log(f"  New study: {mrn}/{dos} (study_uid={study_uid[:32]}..., key={skey})")
    return True


def _handoff_all(received, dirs, source_label):
    """Process all series received during an association (released or aborted)."""
    log(f"Association {source_label}. Series received: {len(received)}")
    for series_uid in received:
        try:
            _handoff_one_series(series_uid, dirs)
        except Exception as e:
            log(f"  !! Handoff failed for {series_uid}: {e}")


def handle_released(event, dirs):
    """Clean release: process all received series."""
    received = getattr(event.assoc, "_received_series", set())
    _handoff_all(received, dirs,
                 f"with {event.assoc.requestor.ae_title} released")
    return 0x0000


def handle_aborted(event, dirs):
    """Aborted association: still process whatever arrived. Each completed
    C-STORE was already acked, so the data is real and committed to disk."""
    received = getattr(event.assoc, "_received_series", set())
    _handoff_all(received, dirs,
                 f"with {event.assoc.requestor.ae_title} ABORTED")


# --- watcher thread (absolute timeout) --------------------------------------

def _read_first_arrival(study_dir):
    f = study_dir / ".first_arrival"
    if not f.exists():
        return None
    try:
        return dt.datetime.strptime(f.read_text().strip(), "%Y-%m-%dT%H-%M-%SZ")
    except Exception:
        return None


def _read_study_uid(study_dir):
    f = study_dir / ".study_uid"
    if f.exists():
        try:
            return f.read_text().strip()
        except Exception:
            return None
    return None


def _promote_study(mrn, dos, study_dir_name, dirs):
    """Move study_pending/<mrn>/<dos>/<studyuid_safe>/ -> staging/...,
    write queue/<skey>.ready."""
    src = dirs["study_pending"] / mrn / dos / study_dir_name
    study_uid = _read_study_uid(src) or study_dir_name  # fallback if marker missing

    skey = study_key(mrn, dos, study_uid)
    dst = dirs["staging"] / mrn / dos / study_dir_name
    dst.parent.mkdir(parents=True, exist_ok=True)

    # Snapshot series before moving (for the log).
    series_uids = sorted(p.name for p in src.iterdir()
                         if p.is_dir())

    shutil.move(str(src), str(dst))

    marker = dirs["queue"] / f"{skey}.ready"
    marker.touch()

    log(f"Study {mrn}/{dos} promoted (key {skey}, {len(series_uids)} series)")
    for sid in series_uids:
        log(f"    {sid}")
    log(f"  marker: queue/{skey}.ready")

    # Try to clean now-empty parents.
    for parent in (dst.parent.parent.parent / mrn / dos,
                   dst.parent.parent.parent / mrn):
        try:
            parent.rmdir()
        except OSError:
            pass


def watcher_loop(dirs, timeout_minutes, poll_seconds, stop_event):
    log(f"Watcher started: timeout={timeout_minutes} min, poll={poll_seconds}s")
    while not stop_event.is_set():
        cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=timeout_minutes)

        # study_pending/<mrn>/<dos>/<studyuid_safe>/
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
                try:
                    study_dirs = [p for p in dos_dir.iterdir() if p.is_dir()]
                except FileNotFoundError:
                    continue
                for study_dir in study_dirs:
                    first = _read_first_arrival(study_dir)
                    if first is None:
                        continue
                    if first < cutoff:
                        try:
                            _promote_study(mrn_dir.name, dos_dir.name,
                                           study_dir.name, dirs)
                        except Exception as e:
                            log(f"!! Promotion failed for "
                                f"{mrn_dir.name}/{dos_dir.name}/{study_dir.name}: {e}")

        stop_event.wait(timeout=poll_seconds)


# --- startup sweep ----------------------------------------------------------

def sweep_orphans(in_dir, orphaned_dir):
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
    parser = argparse.ArgumentParser(description="DICOM SCP (study-level, MRN+DOS+StudyUID).")
    parser.add_argument("--incoming-dir", required=True)
    parser.add_argument("--study-pending-dir", required=True)
    parser.add_argument("--staging-dir", required=True)
    parser.add_argument("--queue-dir", required=True)
    parser.add_argument("--orphaned-dir", required=True)
    parser.add_argument("--quarantine-dir", required=True)
    parser.add_argument("--archive-dir", required=True)
    parser.add_argument("--failed-dir", required=True)
    parser.add_argument("--timeout-minutes", type=float, default=20.0)
    parser.add_argument("--watcher-poll-seconds", type=float, default=60.0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11112)
    parser.add_argument("--ae-title", default="ANY-SCP")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

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

    if dirs["in"].stat().st_dev != dirs["study_pending"].stat().st_dev:
        log("!! WARNING: incoming-dir and study-pending-dir on different FS.")

    sweep_orphans(dirs["in"], dirs["orphaned"])

    if args.debug:
        debug_logger()

    handlers = [
        (evt.EVT_C_STORE, handle_store, [str(dirs["in"])]),
        (evt.EVT_RELEASED, handle_released, [dirs]),
        (evt.EVT_ABORTED,  handle_aborted,  [dirs]),
    ]

    ae = AE(ae_title=args.ae_title)
    for cx in AllStoragePresentationContexts:
        ae.add_supported_context(cx.abstract_syntax, ALL_TRANSFER_SYNTAXES)

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
