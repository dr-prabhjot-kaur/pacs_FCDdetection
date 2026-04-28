"""DICOM Storage SCP.

Receives DICOM C-STORE requests and hands off completed series for
asynchronous processing. Per association:

  1. handle_store writes each incoming file to in/<SeriesInstanceUID>/
     and records the series UID on the association.
  2. handle_released moves each fully-received series from in/ to
     staging/, then drops a <uid>.ready marker in queue/.
  3. SCP returns immediately and is free for the next association.

Nothing is ever deleted. All cleanup is rename/move so failed work
can be inspected.
"""

import argparse
import datetime as dt
import os
import shutil
from pathlib import Path

from pynetdicom import (
    AE, debug_logger, evt,
    AllStoragePresentationContexts, ALL_TRANSFER_SYNTAXES,
)


# --- receive phase -----------------------------------------------------------

def handle_store(event, incomingDir):
    """C-STORE: save the file under in/<SeriesInstanceUID>/<SOPInstanceUID>.

    Also records the SeriesInstanceUID on the association object so
    handle_released knows which series were touched during THIS
    association (and not re-process stale ones).
    """
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


# --- release phase -----------------------------------------------------------

def _timestamp():
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def _handoff_series(series_uid, incomingDir, stagingDir, queueDir):
    """Move in/<uid>/ to staging/<uid>/ and drop queue/<uid>.ready.

    If staging/<uid>/ already exists (duplicate send while a previous
    run is still in flight), the new arrival is renamed with a
    timestamp suffix so nothing is lost and both get processed.
    """
    src = Path(incomingDir) / series_uid
    dst = Path(stagingDir) / series_uid

    if not src.exists():
        print(f"  !! Source missing, skipping: {src}")
        return

    if dst.exists():
        resent_uid = f"{series_uid}_resent_{_timestamp()}"
        dst = Path(stagingDir) / resent_uid
        print(f"  Duplicate detected; staging as {resent_uid}")
        marker_uid = resent_uid
    else:
        marker_uid = series_uid

    # Rename is atomic on the same filesystem (required).
    shutil.move(str(src), str(dst))

    marker = Path(queueDir) / f"{marker_uid}.ready"
    marker.touch()
    print(f"  Staged {marker_uid}, marker written")


def handle_released(event, incomingDir, stagingDir, queueDir):
    """Association released: hand off each series received during this
    association and return. Does NOT wait for processing."""
    assoc = event.assoc
    received = getattr(assoc, "_received_series", set())
    print(f"Association with {assoc.requestor.ae_title} released. "
          f"Series received: {len(received)}")

    for series_uid in received:
        try:
            _handoff_series(series_uid, incomingDir, stagingDir, queueDir)
        except Exception as e:
            print(f"  !! Handoff failed for {series_uid}: {e}")

    return 0x0000


# --- startup sweep -----------------------------------------------------------

def sweep_orphans(incomingDir, orphanedDir):
    """On startup, any series left in in/ from a prior crashed run
    gets moved to orphaned/ for manual review."""
    in_path = Path(incomingDir)
    if not in_path.exists():
        return
    moved = 0
    for entry in in_path.iterdir():
        if entry.is_dir():
            target = Path(orphanedDir) / f"{entry.name}_{_timestamp()}"
            shutil.move(str(entry), str(target))
            moved += 1
    if moved:
        print(f"Swept {moved} orphaned series from in/ to orphaned/")


# --- main --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DICOM Storage SCP (async handoff).")
    parser.add_argument("--incoming-dir", required=True,
                        help="Ephemeral receive buffer. Must share a filesystem with --staging-dir.")
    parser.add_argument("--staging-dir", required=True,
                        help="Where fully-received series wait for processing.")
    parser.add_argument("--queue-dir", required=True,
                        help="Marker files track per-series state.")
    parser.add_argument("--orphaned-dir", required=True,
                        help="Startup sweep target for series left in incoming from prior crashes.")
    parser.add_argument("--host", default="127.0.0.1",
                        help="Bind address. Use 0.0.0.0 when PACS is sending from elsewhere.")
    parser.add_argument("--port", type=int, default=11112)
    parser.add_argument("--ae-title", default="ANY-SCP")
    parser.add_argument("--debug", action="store_true",
                        help="Verbose pynetdicom logging.")
    args = parser.parse_args()

    incomingDir = str(Path(args.incoming_dir).resolve())
    stagingDir = str(Path(args.staging_dir).resolve())
    queueDir = str(Path(args.queue_dir).resolve())
    orphanedDir = str(Path(args.orphaned_dir).resolve())

    for d in (incomingDir, stagingDir, queueDir, orphanedDir):
        os.makedirs(d, exist_ok=True)

    if os.stat(incomingDir).st_dev != os.stat(stagingDir).st_dev:
        print("!! WARNING: incoming-dir and staging-dir are on different "
              "filesystems. Handoff will be a slow copy and will block the "
              "SCP during association release.")

    sweep_orphans(incomingDir, orphanedDir)

    if args.debug:
        debug_logger()

    handlers = [
        (evt.EVT_C_STORE, handle_store, [incomingDir]),
        (evt.EVT_RELEASED, handle_released, [incomingDir, stagingDir, queueDir]),
    ]

    ae = AE(ae_title=args.ae_title)
    for cx in AllStoragePresentationContexts:
        ae.add_supported_context(cx.abstract_syntax, ALL_TRANSFER_SYNTAXES)

    print(f"SCP listening on {args.host}:{args.port} (AE title: {args.ae_title})")
    print(f"  incoming: {incomingDir}")
    print(f"  staging:  {stagingDir}")
    print(f"  queue:    {queueDir}")
    print(f"  orphaned: {orphanedDir}")

    ae.start_server((args.host, args.port), evt_handlers=handlers)


if __name__ == "__main__":
    main()
