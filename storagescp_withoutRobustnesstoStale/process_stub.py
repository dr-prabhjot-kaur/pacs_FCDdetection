"""Placeholder study-level DICOM processor.

Receives --in-dir = a study directory containing one subdir per series.
Reads metadata from one DICOM in each series, writes a study summary.

Replace with your real FCD detection processor when ready. The contract
is just: --in-dir <study_dir> --out-dir <results_dir>.
"""

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

try:
    import pydicom
except ImportError:
    print("pydicom not installed; run: pip install pydicom", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", required=True,
                        help="Study directory containing one subdir per series.")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    series_dirs = sorted(p for p in in_dir.iterdir() if p.is_dir())
    print(f"Processor: study has {len(series_dirs)} series")

    summary = {
        "study_dir": str(in_dir),
        "processed_at": dt.datetime.utcnow().isoformat() + "Z",
        "series_count": len(series_dirs),
        "series": [],
    }

    for series_dir in series_dirs:
        files = sorted(p for p in series_dir.iterdir() if p.is_file())
        entry = {
            "series_uid": series_dir.name,
            "file_count": len(files),
        }
        if files:
            try:
                ds = pydicom.dcmread(str(files[0]), stop_before_pixels=True)
                entry["sop_class"] = str(getattr(ds, "SOPClassUID", ""))
                entry["modality"] = str(getattr(ds, "Modality", ""))
                entry["series_description"] = str(getattr(ds, "SeriesDescription", ""))
                entry["protocol_name"] = str(getattr(ds, "ProtocolName", ""))
                entry["patient_id"] = str(getattr(ds, "PatientID", ""))
                entry["study_date"] = str(getattr(ds, "StudyDate", ""))
                entry["study_uid"] = str(getattr(ds, "StudyInstanceUID", ""))
                entry["frames"] = int(getattr(ds, "NumberOfFrames", 1))
            except Exception as e:
                entry["error"] = str(e)
        summary["series"].append(entry)
        print(f"  {series_dir.name}: {entry.get('series_description', '?')} "
              f"({entry.get('file_count', 0)} files)")

    (out_dir / "study_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Processor: wrote study_summary.json to {out_dir}")


if __name__ == "__main__":
    main()
