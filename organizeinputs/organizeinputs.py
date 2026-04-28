"""Organize a DICOM study into nnUNet + MELD inputs.

Pipeline:
  1. Inventory every series (read tags from a middle slice).
  2. Classify each series (T1w / FLAIR / OTHER) using rules.json.
  3. Apply common gates (ORIGINAL/PRIMARY, voxel band, slice count, etc.).
  4. Pick best candidate per modality (closest to 1mm iso; tiebreak by NORM, then SeriesNumber).
  5. Fail loudly if either modality has no acceptable series.
  6. Convert chosen series to NIfTI via dcm2niix (Singularity).
  7. Copy into nnUNet (Dataset501_FCD/imagesTs/) and MELD (input/MELD_H52_3T_FCD_<subj>/T1+FLAIR/) layouts.

Subject ID = <MRN><DOS> read from the DICOM tags (PatientID + StudyDate).
If a same-day re-scan would collide, the second study gets a "_2" suffix etc.

CLI contract (matches process_series.sh):
    --in-dir  <study_dir>      one subdir per series
    --out-dir <results_dir>    everything written here
    --dcm2niix-sif <path>      Singularity image with dcm2niix
    [--rules <path>]           default: <script_dir>/rules.json
    [--singularity <bin>]      default: singularity
"""

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import pydicom
except ImportError:
    print("pydicom not installed; need it for triage", file=sys.stderr)
    sys.exit(1)


# --- logging ----------------------------------------------------------------

def ts():
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)


# --- DICOM tag reading ------------------------------------------------------

def _middle_file(series_dir):
    """Pick a representative DICOM. We sort by InstanceNumber (cheap header
    read) and take the middle file. First/last can be localizers in some
    sortings, hence not just files[0]."""
    files = [p for p in series_dir.iterdir() if p.is_file()]
    if not files:
        return None
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


def _as_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _join_multival(v):
    """ScanningSequence/SequenceVariant/ImageType arrive as MultiValue.
    Return a backslash-joined uppercased string for easy substring tests."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.upper()
    try:
        return "\\".join(str(x) for x in v).upper()
    except TypeError:
        return str(v).upper()


def _list_multival(v):
    """Same data, but as an uppercase list."""
    if v is None:
        return []
    if isinstance(v, str):
        return [v.upper()]
    try:
        return [str(x).upper() for x in v]
    except TypeError:
        return [str(v).upper()]


def _voxel_dims(ds):
    """Return (px, py, sz) in mm, or Nones for missing dims."""
    px = py = sz = None
    ps = getattr(ds, "PixelSpacing", None)
    if ps is not None and len(ps) >= 2:
        try:
            px = float(ps[0]); py = float(ps[1])
        except (TypeError, ValueError):
            pass
    sbs = getattr(ds, "SpacingBetweenSlices", None)
    st = getattr(ds, "SliceThickness", None)
    for v in (sbs, st):
        if v is None:
            continue
        try:
            sz = float(v); break
        except (TypeError, ValueError):
            pass
    return px, py, sz


def inventory_series(series_dir):
    """Read tags relevant for triage. Never raises."""
    info = {
        "series_uid": series_dir.name,
        "file_count": 0,
        "ok_read": False,
        "modality": None,
        "image_type": [],
        "image_type_str": "",
        "is_normalized": False,
        "tr_ms": None,
        "te_ms": None,
        "ti_ms": None,
        "scanning_sequence": "",
        "sequence_variant": "",
        "sequence_name": "",
        "series_description": "",
        "protocol_name": "",
        "series_number": None,
        "px_mm": None, "py_mm": None, "sz_mm": None,
        "slice_count_estimate": 0,
        "manufacturer": "",
        "patient_id": "",
        "study_date": "",
        "error": None,
    }
    files = [p for p in series_dir.iterdir() if p.is_file()]
    info["file_count"] = len(files)
    info["slice_count_estimate"] = len(files)
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
    info["image_type"] = _list_multival(getattr(ds, "ImageType", None))
    info["image_type_str"] = _join_multival(getattr(ds, "ImageType", None))
    info["is_normalized"] = "NORM" in info["image_type"]
    info["tr_ms"] = _as_float(getattr(ds, "RepetitionTime", None))
    info["te_ms"] = _as_float(getattr(ds, "EchoTime", None))
    info["ti_ms"] = _as_float(getattr(ds, "InversionTime", None))
    info["scanning_sequence"] = _join_multival(getattr(ds, "ScanningSequence", None))
    info["sequence_variant"] = _join_multival(getattr(ds, "SequenceVariant", None))
    info["sequence_name"] = str(getattr(ds, "SequenceName", "")).strip()
    info["series_description"] = str(getattr(ds, "SeriesDescription", "")).strip()
    info["protocol_name"] = str(getattr(ds, "ProtocolName", "")).strip()
    sn = getattr(ds, "SeriesNumber", None)
    try:
        info["series_number"] = int(sn) if sn is not None else None
    except (TypeError, ValueError):
        info["series_number"] = None
    info["px_mm"], info["py_mm"], info["sz_mm"] = _voxel_dims(ds)
    info["manufacturer"] = str(getattr(ds, "Manufacturer", "")).strip()
    info["patient_id"] = str(getattr(ds, "PatientID", "")).strip()
    info["study_date"] = str(getattr(ds, "StudyDate", "")).strip()
    nframes = getattr(ds, "NumberOfFrames", None)
    if nframes is not None:
        try:
            n = int(nframes)
            if n > info["slice_count_estimate"]:
                info["slice_count_estimate"] = n
        except (TypeError, ValueError):
            pass
    return info


# --- token extraction (word-boundary, avoids MPR-inside-MPRAGE) ------------

_TOKEN_RE = re.compile(r"[^A-Z0-9]+")


def _tokens_in(text):
    """Tokenize on non-alphanumerics: 'MPRAGE 3D SAG' -> {MPRAGE, 3D, SAG}."""
    return {t for t in _TOKEN_RE.split(text.upper()) if t}


def _name_blob_tokens(info):
    blob = info["series_description"] + " " + info["protocol_name"]
    return _tokens_in(blob)


# --- gating -----------------------------------------------------------------

def passes_common_gates(info, gates):
    """Apply common gates from rules.json. Returns (ok, reasons)."""
    reasons = []
    if not info["ok_read"]:
        return False, [f"unreadable: {info['error']}"]

    if gates.get("modality") and info["modality"] != gates["modality"].upper():
        reasons.append(f"modality={info['modality']!r} (need {gates['modality']!r})")

    must_inc = [s.upper() for s in gates.get("image_type_must_include", [])]
    for tok in must_inc:
        if tok not in info["image_type"]:
            reasons.append(f"ImageType missing required {tok!r}: {info['image_type']}")

    must_exc = [s.upper() for s in gates.get("image_type_must_exclude", [])]
    for tok in must_exc:
        if tok in info["image_type"]:
            reasons.append(f"ImageType has excluded {tok!r}: {info['image_type']}")

    rmin = gates.get("voxel_mm_min")
    rmax = gates.get("voxel_mm_max")
    if rmin is not None or rmax is not None:
        for label, v in (("px", info["px_mm"]), ("py", info["py_mm"]), ("sz", info["sz_mm"])):
            if v is None:
                reasons.append(f"missing voxel dim {label}")
            else:
                if rmin is not None and v < rmin:
                    reasons.append(f"{label}={v:.3f} < voxel_mm_min={rmin}")
                if rmax is not None and v > rmax:
                    reasons.append(f"{label}={v:.3f} > voxel_mm_max={rmax}")

    if gates.get("min_slices") is not None and \
            info["slice_count_estimate"] < gates["min_slices"]:
        reasons.append(f"slices={info['slice_count_estimate']} < min_slices={gates['min_slices']}")

    neg_tokens = set(t.upper() for t in gates.get("protocol_negative_tokens", []))
    blob_tokens = _name_blob_tokens(info)
    hits = blob_tokens & neg_tokens
    if hits:
        reasons.append(f"protocol contains negative token(s): {sorted(hits)}")

    return (not reasons), reasons


# --- classification (rule-driven) -------------------------------------------

def _rule_matches(info, rule):
    """Single rule block. Returns True if all conditions match. Rules with
    is_fallback=true are NOT matched here — those are handled separately."""
    if rule.get("is_fallback"):
        return False

    # Numeric range checks
    for tag, key_min, key_max in (
            ("tr_ms", "tr_ms_min", "tr_ms_max"),
            ("te_ms", "te_ms_min", "te_ms_max"),
            ("ti_ms", "ti_ms_min", "ti_ms_max"),
    ):
        v = info[tag]
        lo = rule.get(key_min)
        hi = rule.get(key_max)
        if lo is not None:
            if v is None or v < lo:
                return False
        if hi is not None:
            if v is None or v > hi:
                return False

    # ScanningSequence must include all of these (multivalue)
    must_inc_ss = rule.get("scanning_sequence_must_include", [])
    for tok in must_inc_ss:
        if tok.upper() not in info["scanning_sequence"]:
            return False

    # ScanningSequence must exclude any of these
    must_exc_ss = rule.get("scanning_sequence_must_exclude", [])
    for tok in must_exc_ss:
        if tok.upper() in info["scanning_sequence"]:
            return False

    # SequenceVariant — at least one of these must be present
    seqvar_any = rule.get("sequence_variant_must_include_any_of", [])
    if seqvar_any and not any(tok.upper() in info["sequence_variant"] for tok in seqvar_any):
        return False

    # SequenceName regex (Siemens-style fingerprint)
    seqname_re = rule.get("sequence_name_regex")
    if seqname_re:
        if not info["sequence_name"]:
            return False
        if not re.search(seqname_re, info["sequence_name"], re.IGNORECASE):
            return False

    return True


def _fallback_matches(info, rule):
    """Token-based fallback. Used only if no normal rule matched ANY series."""
    if not rule.get("is_fallback"):
        return False
    tokens_any = set(t.upper() for t in rule.get("protocol_tokens_any_of", []))
    if not tokens_any:
        return False
    return bool(_name_blob_tokens(info) & tokens_any)


def classify_series(info, rules_for_modality):
    """Try each rule in order. Returns (matched_rule_name, was_fallback) or
    (None, False) if nothing matched."""
    for rule in rules_for_modality:
        if _rule_matches(info, rule):
            return rule.get("name", "<unnamed>"), False
    return None, False


def classify_with_fallback(entries, rules_for_modality):
    """Run classification on all entries. If no entry matched a non-fallback
    rule, try the fallback rules. Returns the modality name to use, and
    annotates each entry with class_match / used_fallback."""
    primary_hits = 0
    for e in entries:
        match, _ = classify_series(e, rules_for_modality)
        if match:
            e["_match"] = match
            e["_fallback"] = False
            primary_hits += 1

    if primary_hits == 0:
        # Try fallbacks
        for rule in rules_for_modality:
            if not rule.get("is_fallback"):
                continue
            for e in entries:
                if e.get("_match"):
                    continue
                if _fallback_matches(e, rule):
                    e["_match"] = rule.get("name", "<unnamed_fallback>")
                    e["_fallback"] = True


# --- selection scoring ------------------------------------------------------

def isotropy_score(info):
    """Lower is better. Sum of squared deviations from 1.0 mm per dim."""
    px, py, sz = info["px_mm"], info["py_mm"], info["sz_mm"]
    if px is None or py is None or sz is None:
        return float("inf")
    return (px - 1.0) ** 2 + (py - 1.0) ** 2 + (sz - 1.0) ** 2


def selection_key(info, prefs):
    """Sort key: lower is better. Tuple of (score, -norm, -series_number)."""
    parts = []
    if prefs.get("prefer_isotropic", True):
        parts.append(isotropy_score(info))
    if prefs.get("prefer_normalized", True):
        parts.append(0 if info["is_normalized"] else 1)  # NORM first
    if prefs.get("prefer_higher_series_number", True):
        parts.append(-(info["series_number"] or 0))
    return tuple(parts)


# --- subject ID -------------------------------------------------------------

def derive_subject_id(study_in_dir):
    """Read MRN + StudyDate from any series in the study. Strip non-alnum
    just in case the PACS sent something unusual."""
    for sd in sorted(study_in_dir.iterdir()):
        if not sd.is_dir():
            continue
        rep = _middle_file(sd)
        if rep is None:
            continue
        try:
            ds = pydicom.dcmread(str(rep), stop_before_pixels=True,
                                 specific_tags=["PatientID", "StudyDate"])
        except Exception:
            continue
        mrn = re.sub(r"[^A-Za-z0-9]", "", str(getattr(ds, "PatientID", "")))
        dos = re.sub(r"[^A-Za-z0-9]", "", str(getattr(ds, "StudyDate", "")))
        if mrn and dos:
            return mrn + dos, mrn, dos
    return None, None, None


def resolve_collision(base_id, out_root):
    """If <out_root>/meld/input/<prefix><base_id> exists, append _2, _3, ...
    until we find a free slot. Caller passes meld dir; we use that as the
    canonical 'has this subject id been used' check."""
    candidate = base_id
    suffix = 2
    while (out_root / candidate).exists():
        candidate = f"{base_id}_{suffix}"
        suffix += 1
    return candidate


# --- dcm2niix invocation ----------------------------------------------------

def run_dcm2niix(singularity, sif, series_dir, out_dir, basename):
    """Run dcm2niix in Singularity. Returns path to .nii.gz on success."""
    out_dir.mkdir(parents=True, exist_ok=True)
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
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        log(f"  !! dcm2niix timeout after 30 min")
        return None
    if proc.returncode != 0:
        log(f"  !! dcm2niix rc={proc.returncode}")
        log(f"     stdout: {proc.stdout.strip()[-500:]}")
        log(f"     stderr: {proc.stderr.strip()[-500:]}")
        return None
    exact = out_dir / f"{basename}.nii.gz"
    if exact.exists():
        return exact
    candidates = sorted(out_dir.glob(f"{basename}*.nii.gz"),
                        key=lambda p: p.stat().st_size, reverse=True)
    if candidates:
        log(f"  note: dcm2niix split output, taking largest: {candidates[0].name}")
        return candidates[0]
    log(f"  !! no .nii.gz found in {out_dir} after dcm2niix")
    return None


# --- arrangement ------------------------------------------------------------

def arrange_nnunet(t1_nii, flair_nii, subject_id, out_dir, ds_id, ds_name):
    """nnUNet v2: Dataset<id>_<name>/imagesTs/<subj>_0000 + _0001."""
    images_ts = out_dir / "nnunet" / f"Dataset{ds_id:03d}_{ds_name}" / "imagesTs"
    images_ts.mkdir(parents=True, exist_ok=True)
    t1_dst = images_ts / f"{subject_id}_0000.nii.gz"
    fl_dst = images_ts / f"{subject_id}_0001.nii.gz"
    shutil.copy2(t1_nii, t1_dst)
    shutil.copy2(flair_nii, fl_dst)
    log(f"  nnUNet: {t1_dst.relative_to(out_dir)}")
    log(f"  nnUNet: {fl_dst.relative_to(out_dir)}")
    return t1_dst, fl_dst


def arrange_meld(t1_nii, flair_nii, subject_id, prefix, out_dir):
    """MELD: input/<prefix><subj>/{T1,FLAIR}/<modality>.nii.gz."""
    subj_dir = out_dir / "meld" / "input" / f"{prefix}{subject_id}"
    t1_dir = subj_dir / "T1"
    fl_dir = subj_dir / "FLAIR"
    t1_dir.mkdir(parents=True, exist_ok=True)
    fl_dir.mkdir(parents=True, exist_ok=True)
    t1_dst = t1_dir / "T1.nii.gz"
    fl_dst = fl_dir / "FLAIR.nii.gz"
    shutil.copy2(t1_nii, t1_dst)
    shutil.copy2(flair_nii, fl_dst)
    log(f"  MELD:   {t1_dst.relative_to(out_dir)}")
    log(f"  MELD:   {fl_dst.relative_to(out_dir)}")
    return t1_dst, fl_dst


# --- main -------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Organize DICOM study: pick T1w + FLAIR, convert, arrange.")
    p.add_argument("--in-dir", required=True,
                   help="Study dir: one subdir per series.")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--dcm2niix-sif", required=True,
                   help="Path to Singularity image with dcm2niix.")
    p.add_argument("--rules", default=None,
                   help="rules.json path (default: <script_dir>/rules.json).")
    p.add_argument("--singularity", default="singularity",
                   help="Singularity binary (default: singularity).")
    args = p.parse_args()

    in_dir = Path(args.in_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    rules_path = Path(args.rules) if args.rules else Path(__file__).parent / "rules.json"
    if not rules_path.exists():
        log(f"!! rules.json not found: {rules_path}")
        return 9
    rules = json.loads(rules_path.read_text())
    log(f"Rules: {rules_path}")

    if not in_dir.exists():
        log(f"!! in-dir does not exist: {in_dir}")
        return 2

    # --- subject ID ---------------------------------------------------------
    subject_id, mrn, dos = derive_subject_id(in_dir)
    if not subject_id:
        log(f"!! could not derive subject id (no readable PatientID/StudyDate)")
        return 3
    log(f"Subject id base: {subject_id} (MRN={mrn}, DOS={dos})")

    # Collision check: if MELD subj folder for this id already exists
    # under out_dir/meld/input/, suffix _2, _3...
    layout = rules.get("output_layout", {})
    meld_prefix = layout.get("meld_subject_prefix", "")
    meld_root = out_dir / "meld" / "input"
    candidate = subject_id
    suffix = 2
    while (meld_root / f"{meld_prefix}{candidate}").exists():
        candidate = f"{subject_id}_{suffix}"
        suffix += 1
    if candidate != subject_id:
        log(f"  collision detected; using subject id {candidate}")
    subject_id = candidate

    # --- inventory + classify -----------------------------------------------
    series_dirs = sorted(p for p in in_dir.iterdir() if p.is_dir())
    log(f"Triage: {len(series_dirs)} series in {in_dir}")

    entries = []
    for sd in series_dirs:
        info = inventory_series(sd)
        ok, gate_reasons = passes_common_gates(info, rules.get("common_gates", {}))
        info["passes_gates"] = ok
        info["gate_reasons"] = gate_reasons
        info["score"] = isotropy_score(info)
        info["_match"] = None
        info["_fallback"] = False
        entries.append(info)

    # Classify only entries that passed gates. Ungated series are reported
    # but never selected.
    gated = [e for e in entries if e["passes_gates"]]
    classify_with_fallback(gated, rules.get("T1w", []))
    t1_matches = {id(e): (e["_match"], e["_fallback"]) for e in gated if e["_match"]}
    # Reset _match before FLAIR classification so the same entry can be
    # considered as either modality (rare: a misconfigured system might).
    for e in gated:
        e["_t1_match"] = e["_match"]
        e["_t1_fallback"] = e["_fallback"]
        e["_match"] = None
        e["_fallback"] = False
    classify_with_fallback(gated, rules.get("FLAIR", []))
    for e in gated:
        e["_flair_match"] = e["_match"]
        e["_flair_fallback"] = e["_fallback"]
        del e["_match"]; del e["_fallback"]

    # Print per-series triage table
    log("-" * 110)
    for e in entries:
        if e["passes_gates"]:
            t1m = e.get("_t1_match")
            flm = e.get("_flair_match")
            tag = (
                f"T1W:{t1m}" if t1m and not flm else
                f"FLAIR:{flm}" if flm and not t1m else
                f"BOTH:{t1m}/{flm}" if (t1m and flm) else
                "OTHER"
            )
        else:
            tag = "REJECT"
        log(f"  [{tag:>30}] sn={e['series_number']!s:>4} sd={e['series_description'][:32]!r:34} "
            f"voxel=({e['px_mm']},{e['py_mm']},{e['sz_mm']}) n={e['slice_count_estimate']:>4}")
        if not e["passes_gates"]:
            for r in e["gate_reasons"]:
                log(f"      reject: {r}")

    # --- pick best per modality --------------------------------------------
    prefs = rules.get("selection", {})

    t1_cands = [e for e in gated if e.get("_t1_match")]
    flair_cands = [e for e in gated if e.get("_flair_match")]

    t1_pick = sorted(t1_cands, key=lambda e: selection_key(e, prefs))[0] if t1_cands else None
    flair_pick = sorted(flair_cands, key=lambda e: selection_key(e, prefs))[0] if flair_cands else None

    used_t1_fallback = bool(t1_pick and t1_pick.get("_t1_fallback"))
    used_flair_fallback = bool(flair_pick and flair_pick.get("_flair_fallback"))

    # --- report (always written) -------------------------------------------
    report = {
        "subject_id": subject_id,
        "mrn": mrn,
        "dos": dos,
        "in_dir": str(in_dir),
        "rules_file": str(rules_path),
        "processed_at": ts(),
        "series_count": len(entries),
        "series": entries,
        "selection": {
            "t1w_series_uid": t1_pick["series_uid"] if t1_pick else None,
            "t1w_match_rule": t1_pick.get("_t1_match") if t1_pick else None,
            "t1w_used_fallback": used_t1_fallback,
            "flair_series_uid": flair_pick["series_uid"] if flair_pick else None,
            "flair_match_rule": flair_pick.get("_flair_match") if flair_pick else None,
            "flair_used_fallback": used_flair_fallback,
        },
        "status": None,
        "failure_reason": None,
    }

    if t1_pick is None or flair_pick is None:
        missing = []
        if t1_pick is None:
            missing.append("T1w")
        if flair_pick is None:
            missing.append("T2w-FLAIR")
        report["status"] = "failed"
        report["failure_reason"] = (
            f"no acceptable series for: {', '.join(missing)}. "
            f"see series[].gate_reasons / _t1_match / _flair_match."
        )
        (out_dir / "triage_report.json").write_text(
            json.dumps(report, indent=2, default=str))
        log(f"!! Triage failed: {report['failure_reason']}")
        return 4

    log(f"Selected T1w   : {t1_pick['series_uid']}  match={t1_pick['_t1_match']}  "
        f"voxel=({t1_pick['px_mm']},{t1_pick['py_mm']},{t1_pick['sz_mm']})  "
        f"score={t1_pick['score']:.4f}")
    log(f"Selected FLAIR : {flair_pick['series_uid']}  match={flair_pick['_flair_match']}  "
        f"voxel=({flair_pick['px_mm']},{flair_pick['py_mm']},{flair_pick['sz_mm']})  "
        f"score={flair_pick['score']:.4f}")

    # --- convert ------------------------------------------------------------
    nifti_dir = out_dir / "nifti"
    nifti_dir.mkdir(parents=True, exist_ok=True)

    t1_basename = f"T1w_{subject_id}"
    flair_basename = f"FLAIR_{subject_id}"

    t1_nii = run_dcm2niix(args.singularity, args.dcm2niix_sif,
                          in_dir / t1_pick["series_uid"],
                          nifti_dir, t1_basename)
    flair_nii = run_dcm2niix(args.singularity, args.dcm2niix_sif,
                             in_dir / flair_pick["series_uid"],
                             nifti_dir, flair_basename)

    if t1_nii is None or flair_nii is None:
        report["status"] = "failed"
        report["failure_reason"] = (
            f"dcm2niix conversion failed: t1_nii={t1_nii}, flair_nii={flair_nii}")
        (out_dir / "triage_report.json").write_text(
            json.dumps(report, indent=2, default=str))
        log(f"!! {report['failure_reason']}")
        return 5

    report["nifti"] = {
        "t1w": str(t1_nii.relative_to(out_dir)),
        "flair": str(flair_nii.relative_to(out_dir)),
    }

    # --- arrange ------------------------------------------------------------
    nn_t1, nn_fl = arrange_nnunet(
        t1_nii, flair_nii, subject_id, out_dir,
        layout.get("nnunet_dataset_id", 501),
        layout.get("nnunet_dataset_name", "FCD"),
    )
    meld_t1, meld_fl = arrange_meld(
        t1_nii, flair_nii, subject_id,
        layout.get("meld_subject_prefix", "MELD_H52_3T_FCD_"),
        out_dir,
    )

    report["arrangement"] = {
        "nnunet_t1": str(nn_t1.relative_to(out_dir)),
        "nnunet_flair": str(nn_fl.relative_to(out_dir)),
        "meld_t1": str(meld_t1.relative_to(out_dir)),
        "meld_flair": str(meld_fl.relative_to(out_dir)),
    }
    report["status"] = "ok"
    (out_dir / "triage_report.json").write_text(
        json.dumps(report, indent=2, default=str))
    log("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
