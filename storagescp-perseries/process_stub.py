"""Placeholder DICOM processor.

Reads the DICOM files in --in-dir, writes a summary JSON and a
manifest.txt to --out-dir. Swap this for your real processor when
you're ready — the sbatch script just calls whatever binary is in
PROCESSOR. Keep the --in-dir / --out-dir interface.
"""

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

try:
    import pydicom
except ImportError:
    print("pydicom not installed; run: pip install pydicom", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(p for p in in_dir.iterdir() if p.is_file())
    print(f"Processor: found {len(files)} file(s) in {in_dir}")

    summary = {
        "series_dir": str(in_dir),
        "file_count": len(files),
        "processed_at": dt.datetime.utcnow().isoformat() + "Z",
        "files": [],
    }
    manifest_lines = []

    for f in files:
        entry = {"name": f.name, "size": f.stat().st_size}
        try:
            ds = pydicom.dcmread(str(f), stop_before_pixels=True)
            entry["sop_class"] = str(getattr(ds, "SOPClassUID", ""))
            entry["sop_instance"] = str(getattr(ds, "SOPInstanceUID", ""))
            entry["modality"] = str(getattr(ds, "Modality", ""))
            entry["frames"] = int(getattr(ds, "NumberOfFrames", 1))
        except Exception as e:
            entry["error"] = str(e)
        summary["files"].append(entry)
        manifest_lines.append(f"{f.name}\t{entry.get('sop_instance', '')}")

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    (out_dir / "manifest.txt").write_text("\n".join(manifest_lines) + "\n")
    print(f"Processor: wrote summary.json and manifest.txt to {out_dir}")


if __name__ == "__main__":
    main()
