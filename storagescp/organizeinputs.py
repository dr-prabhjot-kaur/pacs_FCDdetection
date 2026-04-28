"""Organize DICOM study inputs: pick best T1w + FLAIR, convert to NIfTI,
arrange into nnUNet and MELD layouts.

Contract (matches process_series.sh):
    --in-dir   <study_dir>   one subdir per series (named by SeriesInstanceUID)
    --out-dir  <results_dir> where to write everything
    --subject-id <skey>      study key from the SCP/submitter
                             (<MRN>_<DOS>_<studyhash>); used as subject id
                             for nnUNet filenames and MELD subject folder.
    --dcm2niix-sif <path>    Singularity image with dcm2niix
    [--singularity <bin>]    default: singularity
    [--site <name>]          MELD site label, default: site01
    [--nnunet-dataset-id N]  default: 501
    [--nnunet-dataset-name]  default: FCD
    [--res-min/--res-max]    voxel range per dim in mm, default 0.8 / 1.2

Pipeline:
    1. Inventory every series; read tags from a middle slice.
    2. Classify each series (T1w / FLAIR / other) from acquisition tags;
       fall back to SeriesDescription tokens only if tag-based finds none.
    3. Gate on ORIGINAL/PRIMARY + voxel-in-band + slice-count + Modality=MR.
    4. Score in-band candidates by closeness to 1.0 mm isotropic;
       tiebreak on higher SeriesNumber.
    5. If either T1w or FLAIR has no acceptable candidate, write
       triage_report.json explaining why, touch .failed-equivalent
       (exit non-zero — process_series.sh writes .failed).
    6. Convert the two chosen series with dcm2niix (singularity).
    7. Copy NIfTI/JSON into both nnUNet and MELD layouts.

Everything that was considered (kept and rejected) is logged in
triage_report.json so a human can see why a series was passed over.
"""

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import pydicom
except ImportError:
    print("pydicom not installed; need it for triage", file=sys.stderr)
    sys.exit(1)


# --- logging helpers --------------------------------------------------------

def ts():
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)


# --- DICOM tag reading ------------------------------------------------------

def _middle_file(series_dir):
    """Pick a representative file. First/last can be localizers in some
    series sortings, so we use the middle by InstanceNumber if available,
    else the middle of the sorted file list."""
    files = [p for p in series_dir.iterdir() if p.is_file()]
    if not files:
        return None

    # Prefer InstanceNumber-based middle (cheap header-only read).
    indexed = []
    for p in files:
        try:
            ds = pydicom.dcmread(str(p), stop_before_pixels=True,
                                 specific_tags=["InstanceNumber"])
            inum = int(getattr(ds, "InstanceNumber", 0) or 0)
            indexed.append((inum, p))
        except Exception:
            indexed.append((0, p))
    indexed.sort(key=lambda t: (t[0], t[1].name))
    return indexed[len(indexed) // 2][1]


def _voxel_dims(ds):
    """Return (px, py, sz) in mm. Any missing -> None for that dim."""
    px = py = sz = None
    ps = getattr(ds, "PixelSpacing", None)
    if ps is not None and len(ps) >= 2:
        try:
            px = float(ps[0])
            py = float(ps[1])
        except (TypeError, ValueError):
            pass
    # Prefer SpacingBetweenSlices if present (handles overlap), else
    # SliceThickness.
    sbs = getattr(ds, "SpacingBetweenSlices", None)
    st = getattr(ds, "SliceThickness", None)
    for v in (sbs, st):
        if v is None:
            continue
        try:
            sz = float(v)
            break
        except (TypeError, ValueError):
            pass
    return px, py, sz


def _image_type_list(ds):
    it = getattr(ds, "ImageType", None)
    if it is None:
        return []
    try:
        return [str(x).upper() for x in it]
    except TypeError:
        return [str(it).upper()]


def inventory_series(series_dir):
    """Read tags relevant for triage. Returns a dict; never raises."""
    info = {
        "series_uid": series_dir.name,
        "file_count": 0,
        "ok_read": False,
        "modality": None,
        "image_type": [],
        "is_original_primary": False,
        "is_derived_or_secondary": False,
        "tr_ms": None,
        "te_ms": None,
        "ti_ms": None,
        "scanning_sequence": None,
        "sequence_variant": None,
        "series_description": "",
        "protocol_name": "",
        "series_number": None,
        "px_mm": None,
        "py_mm": None,
        "sz_mm": None,
        "slice_count_estimate": 0,
        "manufacturer": "",
        "error": None,
    }
    files = [p for p in series_dir.iterdir() if p.is_file()]
    info["file_count"] = len(files)
    info["slice_count_estimate"] = len(files)  # close enough for single-frame
    rep = _middle_file(series_dir)
    if rep is None:
        info["error"] = "empty series directory"
        return info
    try:
        ds = pydicom.dcmread(str(rep), stop_before_pixels=True)
    except Exception as e:
        info["error"] = f"dcmread failed: {e}"
        return info

    info["ok_read"] = True
    info["modality"] = str(getattr(ds, "Modality", "")).upper()
    image_type = _image_type_list(ds)
    info["image_type"] = image_type
    if image_type:
        info["is_original_primary"] = (
            "ORIGINAL" in image_type and "PRIMARY" in image_type
        )
        info["is_derived_or_secondary"] = (
            "DERIVED" in image_type or "SECONDARY" in image_type
        )
    info["tr_ms"] = _as_float(getattr(ds, "RepetitionTime", None))
    info["te_ms"] = _as_float(getattr(ds, "EchoTime", None))
    info["ti_ms"] = _as_float(getattr(ds, "InversionTime", None))
    info["scanning_sequence"] = _join_multival(getattr(ds, "ScanningSequence", None))
    info["sequence_variant"] = _join_multival(getattr(ds, "SequenceVariant", None))
    info["series_description"] = str(getattr(ds, "SeriesDescription", "")).strip()
    info["protocol_name"] = str(getattr(ds, "ProtocolName", "")).strip()
    sn = getattr(ds, "SeriesNumber", None)
    try:
        info["series_number"] = int(sn) if sn is not None else None
    except (TypeError, ValueError):
        info["series_number"] = None
    px, py, sz = _voxel_dims(ds)
    info["px_mm"] = px
    info["py_mm"] = py
    info["sz_mm"] = sz
    info["manufacturer"] = str(getattr(ds, "Manufacturer", "")).strip()
    # NumberOfFrames is set on enhanced/multiframe DICOMs — overrides count.
    nframes = getattr(ds, "NumberOfFrames", None)
    if nframes is not None:
        try:
            n = int(nframes)
            if n > info["slice_count_estimate"]:
                info["slice_count_estimate"] = n
        except (TypeError, ValueError):
            pass
    return info


def _as_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _join_multival(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.upper()
    try:
        return "\\".join(str(x) for x in v).upper()
    except TypeError:
        return str(v).upper()


# --- classification ---------------------------------------------------------

# Tag-based classification. Returns one of: "T1W", "FLAIR", or "OTHER",
# along with a list of human-readable reasons.

def classify_by_tags(info):
    reasons = []
    tr = info["tr_ms"]
    te = info["te_ms"]
    ti = info["ti_ms"]
    scanseq = info["scanning_sequence"] or ""
    seqvar = info["sequence_variant"] or ""

    # FLAIR has the most distinctive signature — check first.
    if ti is not None and 1700 <= ti <= 2900 \
            and te is not None and te >= 70 \
            and tr is not None and tr >= 4500:
        reasons.append(
            f"FLAIR signature: TI={ti:.0f} ms in [1700,2900], "
            f"TE={te:.0f} ms >=70, TR={tr:.0f} ms >=4500"
        )
        return "FLAIR", reasons

    # T1w — looking specifically for 3D MPRAGE / SPGR / BRAVO style.
    # MPRAGE: ScanningSequence has GR & IR, SequenceVariant has SP & MP.
    # SPGR/FSPGR (GE): ScanningSequence GR, SequenceVariant SP & SS or SK.
    is_mprage_like = ("GR" in scanseq and "IR" in scanseq
                      and ("MP" in seqvar or "SP" in seqvar))
    is_spgr_like = ("GR" in scanseq and "SP" in seqvar
                    and "IR" not in scanseq)

    # T1w gates: short TE; either short TR (SPGR) or moderate TR with TI in
    # MPRAGE inversion range (~700-1200 ms).
    short_te = te is not None and te <= 12
    short_tr = tr is not None and tr <= 2500
    mprage_ti = ti is not None and 600 <= ti <= 1300

    if short_te and (short_tr or mprage_ti) and (is_mprage_like or is_spgr_like):
        bits = [
            f"T1w signature: TE={te:.1f} ms <=12",
            f"TR={tr:.0f} ms" if tr is not None else "TR=?",
        ]
        if ti is not None:
            bits.append(f"TI={ti:.0f} ms")
        bits.append(f"ScanSeq={scanseq}, SeqVar={seqvar}")
        reasons.append("; ".join(bits))
        return "T1W", reasons

    reasons.append(
        f"no tag match (TR={tr}, TE={te}, TI={ti}, "
        f"ScanSeq={scanseq}, SeqVar={seqvar})"
    )
    return "OTHER", reasons


# Heuristic-only fallback: look at SeriesDescription / ProtocolName.
# Used only if NO series classified as T1w (or FLAIR) by tags.

T1W_NAME_TOKENS = ("MPRAGE", "BRAVO", "SPGR", "FSPGR", "TFL3D",
                   "MP2RAGE")
FLAIR_NAME_TOKENS = ("FLAIR",)
NEGATIVE_TOKENS = ("MIP", "MPR", "DERIVED", "REFORMAT", "SCREEN", "REPORT",
                   "CAL", "SCOUT", "LOCALIZER", "T2STAR", "DWI", "ADC",
                   "PERF", "PWI", "ASL", "SWI", "FA", "TRACE", "EXP", "B0",
                   "B1000", "DTI", "POST", "GAD")  # POST/GAD: post-contrast variants


_TOKEN_SPLIT_RE = None  # lazy-compiled


def _tokens_in(blob):
    """Yield uppercase alphanumeric/underscore-separated tokens from blob.
    Splits on anything non-alphanumeric so 'MPRAGE 3D SAG +C' becomes
    ['MPRAGE','3D','SAG','C']. Avoids the MPR-inside-MPRAGE substring trap."""
    global _TOKEN_SPLIT_RE
    if _TOKEN_SPLIT_RE is None:
        import re
        _TOKEN_SPLIT_RE = re.compile(r"[^A-Z0-9]+")
    return [t for t in _TOKEN_SPLIT_RE.split(blob.upper()) if t]


def name_match(info, tokens):
    """Token-level (word-boundary) match. Rejects if ANY negative token
    appears, accepts if ANY positive token appears.

    Note tokens like 'T2-FLAIR' or 'MP-RAGE' get split into 'T2'+'FLAIR'
    or 'MP'+'RAGE' on both sides, which is fine because we'd hit the
    plain 'FLAIR' / 'MPRAGE' entries anyway. Multi-token tokens like
    'MP-RAGE' in our list are effectively dead entries; the plain forms
    do the work."""
    blob = info["series_description"] + " " + info["protocol_name"]
    blob_tokens = set(_tokens_in(blob))
    if blob_tokens & set(NEGATIVE_TOKENS):
        return False
    return bool(blob_tokens & set(t for t in tokens if "-" not in t and "_" not in t))


def heuristic_class(info):
    if name_match(info, FLAIR_NAME_TOKENS):
        return "FLAIR"
    if name_match(info, T1W_NAME_TOKENS):
        return "T1W"
    return "OTHER"


# --- gating & scoring -------------------------------------------------------

def passes_gates(info, res_min, res_max, min_slices):
    """Common gates: MR, ORIGINAL/PRIMARY, voxels in band, enough slices.
    Returns (ok, list_of_reject_reasons_if_not_ok)."""
    reasons = []
    if not info["ok_read"]:
        return False, [f"unreadable: {info['error']}"]
    if info["modality"] != "MR":
        reasons.append(f"modality={info['modality']!r} (not MR)")
    if info["is_derived_or_secondary"]:
        reasons.append(f"ImageType has DERIVED/SECONDARY: {info['image_type']}")
    if not info["is_original_primary"]:
        reasons.append(
            f"ImageType not ORIGINAL\\PRIMARY: {info['image_type']}"
        )
    px, py, sz = info["px_mm"], info["py_mm"], info["sz_mm"]
    if px is None or py is None or sz is None:
        reasons.append(f"missing voxel dim: px={px}, py={py}, sz={sz}")
    else:
        for label, v in (("px", px), ("py", py), ("sz", sz)):
            if v < res_min or v > res_max:
                reasons.append(
                    f"{label}={v:.3f} mm outside [{res_min},{res_max}]"
                )
    if info["slice_count_estimate"] < min_slices:
        reasons.append(
            f"slice_count={info['slice_count_estimate']} < min {min_slices}"
        )
    if reasons:
        return False, reasons
    return True, []


def isotropy_score(info):
    """Lower is better. Sum of squared deviations from 1.0 mm per dim."""
    px = info["px_mm"]
    py = info["py_mm"]
    sz = info["sz_mm"]
    if px is None or py is None or sz is None:
        return float("inf")
    return (px - 1.0) ** 2 + (py - 1.0) ** 2 + (sz - 1.0) ** 2


# --- dcm2niix invocation ----------------------------------------------------

def run_dcm2niix(singularity, sif, series_dir, out_dir, basename):
    """Run dcm2niix inside a Singularity image. Returns path to the .nii.gz
    produced (or None on failure)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    # Bind both series_dir and out_dir; -f basename so output is predictable.
    # -b y JSON, -z y gzip, -w 1 overwrite, -i n keep all (we run on a single
    # series dir so there's only one).
    cmd = [
        singularity, "exec",
        "--bind", f"{series_dir.parent}:{series_dir.parent}",
        "--bind", f"{out_dir}:{out_dir}",
        sif,
        "dcm2niix",
        "-b", "y", "-z", "y", "-w", "1",
        "-f", basename,
        "-o", str(out_dir),
        str(series_dir),
    ]
    log(f"  dcm2niix: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if proc.returncode != 0:
        log(f"  !! dcm2niix rc={proc.returncode}")
        log(f"     stdout: {proc.stdout.strip()[-500:]}")
        log(f"     stderr: {proc.stderr.strip()[-500:]}")
        return None
    # dcm2niix may produce <basename>.nii.gz, or with suffixes if it split.
    # Prefer the exact basename match; otherwise take the largest .nii.gz.
    exact = out_dir / f"{basename}.nii.gz"
    if exact.exists():
        return exact
    candidates = sorted(out_dir.glob(f"{basename}*.nii.gz"),
                        key=lambda p: p.stat().st_size,
                        reverse=True)
    if candidates:
        log(f"  note: dcm2niix produced split outputs, taking largest: "
            f"{candidates[0].name}")
        return candidates[0]
    log(f"  !! no .nii.gz found in {out_dir} after dcm2niix")
    return None


# --- arrangement (nnUNet + MELD) --------------------------------------------

def arrange_nnunet(t1_nii, flair_nii, subject_id, out_dir,
                   dataset_id, dataset_name):
    """nnUNet v2 layout: Dataset<id>_<name>/imagesTs/<subject>_0000.nii.gz
    and _0001.nii.gz. T1w is channel 0, FLAIR is channel 1."""
    ds_dir = out_dir / "nnunet" / f"Dataset{dataset_id:03d}_{dataset_name}"
    images_ts = ds_dir / "imagesTs"
    images_ts.mkdir(parents=True, exist_ok=True)
    t1_dst = images_ts / f"{subject_id}_0000.nii.gz"
    fl_dst = images_ts / f"{subject_id}_0001.nii.gz"
    shutil.copy2(t1_nii, t1_dst)
    shutil.copy2(flair_nii, fl_dst)
    log(f"  nnUNet: {t1_dst.relative_to(out_dir)}")
    log(f"  nnUNet: {fl_dst.relative_to(out_dir)}")
    return t1_dst, fl_dst


def arrange_meld(t1_nii, flair_nii, subject_id, site, out_dir):
    """MELD-graph input layout: meld/input/<site>_<subject>/T1.nii.gz +
    FLAIR.nii.gz. Subject id is prefixed with site to match MELD's
    expectation that subjects start with the site code."""
    subj_dir = out_dir / "meld" / "input" / f"{site}_{subject_id}"
    subj_dir.mkdir(parents=True, exist_ok=True)
    t1_dst = subj_dir / "T1.nii.gz"
    fl_dst = subj_dir / "FLAIR.nii.gz"
    shutil.copy2(t1_nii, t1_dst)
    shutil.copy2(flair_nii, fl_dst)
    log(f"  MELD:   {t1_dst.relative_to(out_dir)}")
    log(f"  MELD:   {fl_dst.relative_to(out_dir)}")
    return t1_dst, fl_dst


# --- main -------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Organize a DICOM study: pick best T1w + FLAIR, "
                    "convert to NIfTI, arrange into nnUNet + MELD layouts.")
    p.add_argument("--in-dir", required=True,
                   help="Study dir: one subdir per series.")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--subject-id", required=True,
                   help="Subject id (study key: <MRN>_<DOS>_<studyhash>).")
    p.add_argument("--dcm2niix-sif", required=True,
                   help="Path to a Singularity image with dcm2niix on PATH.")
    p.add_argument("--singularity", default="singularity",
                   help="Singularity binary (default: singularity).")
    p.add_argument("--site", default="site01",
                   help="MELD site label, default site01.")
    p.add_argument("--nnunet-dataset-id", type=int, default=501)
    p.add_argument("--nnunet-dataset-name", default="FCD")
    p.add_argument("--res-min", type=float, default=0.8,
                   help="Min voxel dim in mm (per axis), default 0.8.")
    p.add_argument("--res-max", type=float, default=1.2,
                   help="Max voxel dim in mm (per axis), default 1.2.")
    p.add_argument("--min-slices", type=int, default=120,
                   help="Min slice count to consider 3D, default 120.")
    args = p.parse_args()

    in_dir = Path(args.in_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_dir.exists():
        log(f"!! in-dir does not exist: {in_dir}")
        return 2

    series_dirs = sorted(p for p in in_dir.iterdir() if p.is_dir())
    log(f"Triage: {len(series_dirs)} series in {in_dir}")
    log(f"  subject id:    {args.subject_id}")
    log(f"  voxel band:    [{args.res_min}, {args.res_max}] mm/dim")
    log(f"  min slices:    {args.min_slices}")
    log(f"  dcm2niix sif:  {args.dcm2niix_sif}")

    # --- 1. inventory + classify --------------------------------------------
    entries = []
    for sd in series_dirs:
        info = inventory_series(sd)
        cls, reasons = classify_by_tags(info)
        info["class_by_tags"] = cls
        info["class_reasons"] = reasons
        ok, gate_reasons = passes_gates(info, args.res_min, args.res_max,
                                        args.min_slices)
        info["passes_gates"] = ok
        info["gate_reasons"] = gate_reasons
        info["score"] = isotropy_score(info)
        entries.append(info)

        tag = cls if ok else f"{cls}/REJECT"
        log(f"  [{tag:>12}] {sd.name[:32]}... "
            f"sd='{info['series_description'][:32]}' "
            f"voxel=({info['px_mm']},{info['py_mm']},{info['sz_mm']}) "
            f"n={info['slice_count_estimate']}")
        if not ok:
            for r in gate_reasons:
                log(f"      reject: {r}")

    # --- 2. tag-based selection ---------------------------------------------
    used_fallback_t1 = False
    used_fallback_flair = False

    def pick_best(class_label):
        cands = [e for e in entries
                 if e["class_by_tags"] == class_label and e["passes_gates"]]
        if not cands:
            return None
        cands.sort(key=lambda e: (e["score"],
                                  -(e["series_number"] or 0)))
        return cands[0]

    t1_pick = pick_best("T1W")
    flair_pick = pick_best("FLAIR")

    # --- 3. heuristic fallback (only if tag selection found nothing) --------
    if t1_pick is None:
        for e in entries:
            if not e["passes_gates"]:
                continue
            if heuristic_class(e) == "T1W":
                e["class_by_heuristic"] = "T1W"
        cands = [e for e in entries
                 if e.get("class_by_heuristic") == "T1W"]
        if cands:
            cands.sort(key=lambda e: (e["score"],
                                      -(e["series_number"] or 0)))
            t1_pick = cands[0]
            used_fallback_t1 = True
            log(f"  fallback heuristic picked T1w: {t1_pick['series_uid'][:32]}")

    if flair_pick is None:
        for e in entries:
            if not e["passes_gates"]:
                continue
            if heuristic_class(e) == "FLAIR":
                e["class_by_heuristic"] = "FLAIR"
        cands = [e for e in entries
                 if e.get("class_by_heuristic") == "FLAIR"]
        if cands:
            cands.sort(key=lambda e: (e["score"],
                                      -(e["series_number"] or 0)))
            flair_pick = cands[0]
            used_fallback_flair = True
            log(f"  fallback heuristic picked FLAIR: "
                f"{flair_pick['series_uid'][:32]}")

    # --- 4. report (always written) -----------------------------------------
    report = {
        "subject_id": args.subject_id,
        "site": args.site,
        "in_dir": str(in_dir),
        "processed_at": ts(),
        "thresholds": {
            "res_min_mm": args.res_min,
            "res_max_mm": args.res_max,
            "min_slices": args.min_slices,
        },
        "series_count": len(entries),
        "series": entries,
        "selection": {
            "t1w_series_uid": t1_pick["series_uid"] if t1_pick else None,
            "flair_series_uid": flair_pick["series_uid"] if flair_pick else None,
            "t1w_used_heuristic_fallback": used_fallback_t1,
            "flair_used_heuristic_fallback": used_fallback_flair,
        },
        "status": None,
        "failure_reason": None,
    }

    # --- 5. fail-fast if either is missing ----------------------------------
    if t1_pick is None or flair_pick is None:
        missing = []
        if t1_pick is None:
            missing.append("T1w")
        if flair_pick is None:
            missing.append("T2w-FLAIR")
        report["status"] = "failed"
        report["failure_reason"] = (
            f"no acceptable series for: {', '.join(missing)}. "
            f"see series[].class_by_tags / passes_gates / gate_reasons."
        )
        (out_dir / "triage_report.json").write_text(
            json.dumps(report, indent=2, default=str))
        log(f"!! Triage failed: {report['failure_reason']}")
        return 4

    log(f"Selected T1w   : {t1_pick['series_uid']}  "
        f"(score={t1_pick['score']:.4f}, "
        f"voxel=({t1_pick['px_mm']},{t1_pick['py_mm']},{t1_pick['sz_mm']}))")
    log(f"Selected FLAIR : {flair_pick['series_uid']}  "
        f"(score={flair_pick['score']:.4f}, "
        f"voxel=({flair_pick['px_mm']},{flair_pick['py_mm']},{flair_pick['sz_mm']}))")

    # --- 6. convert with dcm2niix -------------------------------------------
    nifti_dir = out_dir / "nifti"
    nifti_dir.mkdir(parents=True, exist_ok=True)

    t1_basename = f"T1w_s{t1_pick['series_number'] or 0:04d}"
    flair_basename = f"FLAIR_s{flair_pick['series_number'] or 0:04d}"

    t1_nii = run_dcm2niix(args.singularity, args.dcm2niix_sif,
                          in_dir / t1_pick["series_uid"],
                          nifti_dir, t1_basename)
    flair_nii = run_dcm2niix(args.singularity, args.dcm2niix_sif,
                             in_dir / flair_pick["series_uid"],
                             nifti_dir, flair_basename)

    if t1_nii is None or flair_nii is None:
        report["status"] = "failed"
        report["failure_reason"] = (
            f"dcm2niix conversion failed: "
            f"t1_nii={t1_nii}, flair_nii={flair_nii}"
        )
        (out_dir / "triage_report.json").write_text(
            json.dumps(report, indent=2, default=str))
        log(f"!! {report['failure_reason']}")
        return 5

    report["nifti"] = {
        "t1w": str(t1_nii.relative_to(out_dir)),
        "flair": str(flair_nii.relative_to(out_dir)),
    }

    # --- 7. arrange into nnUNet and MELD layouts ----------------------------
    nn_t1, nn_fl = arrange_nnunet(t1_nii, flair_nii, args.subject_id, out_dir,
                                  args.nnunet_dataset_id,
                                  args.nnunet_dataset_name)
    meld_t1, meld_fl = arrange_meld(t1_nii, flair_nii, args.subject_id,
                                    args.site, out_dir)

    report["arrangement"] = {
        "nnunet_t1": str(nn_t1.relative_to(out_dir)),
        "nnunet_flair": str(nn_fl.relative_to(out_dir)),
        "meld_t1": str(meld_t1.relative_to(out_dir)),
        "meld_flair": str(meld_fl.relative_to(out_dir)),
    }
    report["status"] = "ok"
    (out_dir / "triage_report.json").write_text(
        json.dumps(report, indent=2, default=str))
    log("Triage + conversion + arrangement complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
