#!/bin/bash
# Unzip all Flywheel DICOM packages for subject 5834888 and dcmsend
set -u

SRC=/fileserver/Rad-Warfield-e2/Groups/Imp-Recons/prabhjot/datasets/thresherepilepsy/flywheel20230224/flywheel/ThrasherEpilepsy/SUBJECTS/5834888/SESSIONS/BCH3T/ACQUISITIONS
DST=/tmp/dcmsend_5834888

rm -rf "$DST"
mkdir -p "$DST"

echo "Source: $SRC"
echo "Dest:   $DST"
echo

# --- Extract all Flywheel zips ---
n_series=0
total=0
for series_dir in "$SRC"/*/; do
    [ -d "$series_dir" ] || continue
    series=$(basename "$series_dir")
    files_dir="$series_dir/FILES"
    [ -d "$files_dir" ] || continue

    out="$DST/$series"
    mkdir -p "$out"

    # Unzip all .dicom.zip files in this series, flat into out/
    n_zips=0
    for z in "$files_dir"/*.dicom.zip; do
        [ -f "$z" ] || continue
        unzip -j -o -q "$z" -d "$out/" 2>/dev/null
        n_zips=$((n_zips + 1))
    done

    # Also copy any plain .dcm files
    if compgen -G "$files_dir/*.dcm" > /dev/null; then
        cp "$files_dir"/*.dcm "$out/" 2>/dev/null
    fi

    n=$(find "$out" -type f | wc -l)
    if [ "$n" -gt 0 ]; then
        n_series=$((n_series + 1))
        total=$((total + n))
        echo "  $series: $n_zips zips -> $n files"
    else
        rmdir "$out" 2>/dev/null
    fi
done

echo
echo "============================================================"
echo "Series: $n_series   Total files: $total"
du -sh "$DST"
echo "============================================================"

[ "$total" -eq 0 ] && { echo "!! Nothing to send."; exit 1; }

# --- Send ---
echo
echo "Sending to 127.0.0.1:11112 ..."
docker run --network=host --rm -v "$DST":/data \
    prabhjotkaur1991/dicom-tools-prabh:latest \
    dcmsend --verbose --scan-directories --recurse --no-halt \
    127.0.0.1 11112 -aet COMPRES /data/

echo "Done."

