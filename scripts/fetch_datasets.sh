#!/usr/bin/env bash
# M4 Stage D dataset fetcher (coding plan Stage D "数据集工具链" row).
#
# D-1 DECISION PENDING (2026-09-03, Stage D round 1): the coding plan's first
# Stage D task — ftp reachability from GitHub Actions runners for
# ftp.irisa.fr — has not been executed yet (this host has no working ftp
# either; the D-1 runner test is handled by the main orchestrator). This
# script is therefore deliberately scheme-agnostic: the defaults below are
# the official ftp URLs, and every dataset can be redirected to any
# http(s)/ftp mirror via an environment override. Once D-1 lands (plan 1:
# ftp direct + actions/cache; plan 2: self-hosted GitHub release), backfill
# the default URLs here and record the conclusion in
# docs/phase2-m4-benchmarks.md.
#
# Usage:
#   scripts/fetch_datasets.sh siftsmall|sift|gist|all
#
# Overrides:
#   M4_DATASETS_DIR — root directory holding archives + extracted datasets
#       (default: <workspace>/datasets). Used by tests and one-off runs.
#   M4_DATASET_URL_SIFTSMALL / M4_DATASET_URL_SIFT / M4_DATASET_URL_GIST
#       — any http(s):// or ftp:// URL of the corresponding .tar.gz.
#   M4_DATASET_SHA256_SIFT / M4_DATASET_SHA256_GIST — expected SHA-256 for
#       sift/gist. There is currently NO trusted official digest for these:
#       texmex publishes only an MD5SUM file on the ftp host itself, which
#       is unreachable from here (2026-09-03). If unset, a loud WARNING is
#       printed and the fetch proceeds unverified. siftsmall's digest is
#       pinned in this script and is deliberately NOT overridable via
#       environment — it is the recall gate's defense line and must not be
#       bypassable.
#
# siftsmall pinned SHA-256 provenance (2026-09-03, Stage D): the main
# orchestrator retrieved this copy from the HuggingFace mirror
# vecdata/siftsmall, verified archive structure (fvecs/ivecs dims/counts)
# and cross-checked recall against the recall gate before pinning.
#
# Integrity & atomic publish:
#   - downloads go to <archive>.part and are mv-renamed to the final name
#     only after completion — a partial download is never published;
#   - a mismatched SHA-256 deletes the archive and exits loudly;
#   - extraction goes to a staging directory; the flattened payload lands in
#     datasets/<name>/ and a .done manifest (archive sha256 + per-file
#     sha256/size/name of every payload file) is written only after every
#     post-check passes;
#   - the skip rule keys off .done — honored ONLY when its recorded archive
#     sha256 matches the current expectation (round-3 note in fetch_one) AND
#     the payload still matches the manifest file-for-file (round-5 note);
#   - stale staging dirs and .part files from interrupted runs are cleaned
#     up at the start of the next run.
#
# Archive layout handling (P1-1 fix): the official texmex archives carry a
# top-level directory named after the dataset (siftsmall.tar.gz extracts to
# siftsmall/...), so extracting straight into datasets/<name>/ produced a
# doubled datasets/<name>/<name>/ layout and the post-check failed (the
# local datasets/ copy predating this fix was extracted by hand, bypassing
# the script). We flatten: if staging holds exactly one entry and it is a
# directory named like the dataset, its contents are used; otherwise
# staging's contents are used as-is.

set -euo pipefail
shopt -s nullglob

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATASETS_DIR="${M4_DATASETS_DIR:-$ROOT/datasets}"

SIFTSMALL_SHA256="987b526d24e749082ba27ee8068003836eb17a61b34f09b4db865750f8a43487"

default_url() {
    case "$1" in
        siftsmall) echo "ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz" ;;
        sift)      echo "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz" ;;
        gist)      echo "ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz" ;;
        *) return 1 ;;
    esac
}

expected_sha256() {
    case "$1" in
        siftsmall) echo "$SIFTSMALL_SHA256" ;;
        sift)      echo "${M4_DATASET_SHA256_SIFT:-}" ;;
        gist)      echo "${M4_DATASET_SHA256_GIST:-}" ;;
    esac
}

sha256_of() {
    # shasum -a 256 exists on both macOS and Linux.
    shasum -a 256 "$1" | awk '{print $1}'
}

byte_size() {
    stat -f%z "$1" 2>/dev/null || stat -c%s "$1"
}

# Payload completeness check: the recall harness keys off these three file
# classes; each must exist and be non-empty. Single implementation shared by
# the skip-branch verification and the post-extraction post-check. Prints
# the first missing/empty file to stderr and returns 1.
check_payload() {
    local dir="$1" suffix f
    local matches
    for suffix in _base.fvecs _query.fvecs _groundtruth.ivecs; do
        matches=( "$dir"/*"$suffix" )
        if [ "${#matches[@]}" -eq 0 ]; then
            echo "no *$suffix found under $dir" >&2
            return 1
        fi
        for f in "${matches[@]}"; do
            if [ ! -s "$f" ]; then
                echo "$f is empty" >&2
                return 1
            fi
        done
    done
    return 0
}

# --- .done manifest (v2) ---------------------------------------------------
# Format (2026-09-07, Stage D round 5):
#   line 1:  magic header "# m4-fetch done manifest v2"
#   line 2:  "archive-sha256: <sha256 of the .tar.gz>"
#   lines 3+: "<file sha256> <byte size> <file name>" — one per payload file,
#             sorted by file name, .done itself excluded. Payload is flat and
#             file names are space-free (texmex corpus), so plain
#             space-separated fields are unambiguous.

manifest_archive_sha() {
    # Prints the recorded archive sha256 of a v2 manifest; prints nothing
    # for the old (round-2..4) single-line format.
    if [ "$(head -n 1 "$1")" = "# m4-fetch done manifest v2" ]; then
        sed -n '2s/^archive-sha256: //p' "$1"
    fi
}

write_done_manifest() {
    local dir="$1" archive_sha="$2" manifest="$3"
    local fname
    {
        echo "# m4-fetch done manifest v2"
        echo "archive-sha256: $archive_sha"
        for fname in $(ls -A "$dir" | grep -v '^\.done$' | sort); do
            printf '%s %s %s\n' "$(sha256_of "$dir/$fname")" "$(byte_size "$dir/$fname")" "$fname"
        done
    } > "$manifest"
}

verify_done_manifest() {
    local dir="$1" manifest="$2"
    # File-set comparison first: missing AND extra files both fail.
    local recorded_files actual_files
    recorded_files="$(tail -n +3 "$manifest" | awk '{print $3}' | sort)"
    actual_files="$(ls -A "$dir" | grep -v '^\.done$' | sort || true)"
    if [ "$recorded_files" != "$actual_files" ]; then
        echo "payload file set under $dir differs from the .done manifest:" >&2
        diff <(echo "$recorded_files") <(echo "$actual_files") >&2 || true
        return 1
    fi
    # Per-file sha256; the recorded byte size rides along as a redundant
    # quick reference inside the manifest.
    local sha size fname actual
    while read -r sha size fname; do
        actual="$(sha256_of "$dir/$fname")"
        if [ "$actual" != "$sha" ]; then
            echo "$dir/$fname corrupted: sha256 $actual != manifest $sha (recorded size ${size}B)" >&2
            return 1
        fi
    done < <(tail -n +3 "$manifest")
    return 0
}

fetch_one() {
    local name="$1"
    local upper
    upper="$(echo "$name" | tr '[:lower:]' '[:upper:]')"
    local override_var="M4_DATASET_URL_$upper"
    local url="${!override_var:-$(default_url "$name")}"
    local dir="$DATASETS_DIR/$name"
    local archive="$DATASETS_DIR/$name.tar.gz"
    local part="$archive.part"
    local staging="$DATASETS_DIR/.$name.staging"
    local done_marker="$dir/.done"

    # Expected SHA-256, resolved once at a single point: the pinned digest
    # for siftsmall, else M4_DATASET_SHA256_<NAME>; neither = no expectation
    # (the WARNING semantics below are unchanged).
    #
    # 2026-09-07 Stage D round 3 fix: the skip rule previously trusted .done
    # unconditionally, which let .done BYPASS the digest defense line —
    # fetch sift with no digest set (WARNING path lands .done), then set a
    # deliberately wrong M4_DATASET_SHA256_SIFT, and the script still
    # skipped with exit 0. Now a .done whose recorded archive sha256 differs
    # from the current expectation is treated as STALE: the payload dir,
    # .done and the archive are wiped and the full download/verify/extract
    # flow reruns; if the expectation itself is wrong, the post-download
    # integrity check below fails loudly — fail loud, never brick, never
    # silently skip.
    local expected
    expected="$(expected_sha256 "$name")"

    # Skip rule: .done is honored only when (round 3) its recorded archive
    # sha256 still matches the current expectation — with no expectation the
    # recorded sha alone suffices, unchanged — and (rounds 4+5) the payload
    # itself still verifies against the manifest.
    #
    # 2026-09-07 Stage D round 4: the sha match only proves the ARCHIVE is
    # the expected one; .done proves "the last extraction finished", not
    # "the payload is intact right now" (deleted *_query.fvecs went
    # unnoticed until the gate test blew up at parse time).
    #
    # 2026-09-07 Stage D round 5: round 4's exist-and-non-empty check still
    # did not prove INTACT — external audit truncated siftsmall_query.fvecs
    # from 51,600B to 4B and the script still reported "payload verified —
    # skipping". Non-empty != complete; truncation / same-length corruption
    # need content digests. .done is therefore now a v2 manifest (format
    # above) and the skip branch verifies the payload FILE-FOR-FILE: file
    # set first, then per-file sha256. Any mismatch (missing / extra /
    # truncated / content-changed) takes the same INCOMPLETE path as round
    # 4: wipe payload + .done, KEEP the verified archive, re-extract.
    #
    # Performance: skip-branch verification hashes the full payload
    # (siftsmall ~17MB — milliseconds; gist ~4GB — seconds). It runs only on
    # the skip-decision path; on CI a cache hit means this fetch step runs
    # anyway, and this is the correctness defense line.
    if [ -f "$done_marker" ]; then
        local recorded
        recorded="$(manifest_archive_sha "$done_marker")"
        if [ -z "$recorded" ]; then
            # Compatibility: a pre-v2 .done holds only the archive sha256 on
            # a single line and cannot verify the payload file-for-file —
            # treat it as untrusted, take the INCOMPLETE path (archive
            # kept), and rebuild .done in the v2 format on re-extraction.
            # Deliberately no migration logic beyond this.
            echo "[$name] INCOMPLETE: .done predates the v2 manifest format (archive sha only) — untrusted" >&2
            echo "[$name] wiping payload dir and .done; re-extracting from the archive (kept) and rebuilding the manifest" >&2
            rm -rf "$dir"
        elif [ -n "$expected" ] && [ "$recorded" != "$expected" ]; then
            echo "[$name] STALE: .done records archive sha256 $recorded but current expectation is $expected" >&2
            echo "[$name] wiping payload dir, .done and archive; re-running the full download/verify/extract flow" >&2
            rm -rf "$dir" "$archive"
        elif ! verify_done_manifest "$dir" "$done_marker"; then
            echo "[$name] INCOMPLETE: .done present but payload verification failed (reason above)" >&2
            echo "[$name] wiping payload dir and .done; re-extracting from the verified archive (kept)" >&2
            rm -rf "$dir"
        else
            echo "[$name] .done manifest present (archive sha256: $recorded), payload verified file-for-file — skipping"
            return 0
        fi
    fi

    mkdir -p "$DATASETS_DIR"

    # Clean up leftovers from an interrupted previous run before doing
    # anything else.
    rm -rf "$staging" "$part"

    if [ -s "$archive" ]; then
        echo "[$name] reusing existing archive $archive ($(byte_size "$archive") bytes)"
    else
        echo "[$name] downloading $url"
        # curl handles both ftp:// and http(s)://; -f fails loudly on HTTP
        # errors, -L follows redirects (self-hosted release scenario).
        # Download to .part first; the mv publishes the archive atomically
        # once the download completed.
        curl -fSL --retry 3 -o "$part" "$url"
        mv "$part" "$archive"
    fi

    # Integrity check. Runs for fresh downloads AND for the reuse path
    # (.done absent but archive present): a reused archive must pass the
    # same expected-sha check before extraction — this was already the case
    # and is confirmed here (round 3). `expected` was resolved above.
    local archive_sha
    archive_sha="$(sha256_of "$archive")"
    if [ -n "$expected" ]; then
        if [ "$archive_sha" != "$expected" ]; then
            echo "[$name] ERROR: SHA-256 mismatch for $archive" >&2
            echo "[$name]   expected: $expected" >&2
            echo "[$name]   actual:   $archive_sha" >&2
            echo "[$name] deleting the archive; re-run to fetch a fresh copy" >&2
            rm -f "$archive"
            exit 1
        fi
        echo "[$name] SHA-256 verified ($archive_sha)"
    else
        echo "[$name] WARNING: no trusted SHA-256 available for $name — proceeding UNVERIFIED" >&2
        echo "[$name] (official texmex MD5SUM is ftp-hosted and unreachable; set M4_DATASET_SHA256_$upper to pin one)" >&2
    fi

    echo "[$name] extracting into staging $staging"
    mkdir -p "$staging"
    tar -xzf "$archive" -C "$staging"

    # Flatten the "archive carries a top-level directory" layout: if staging
    # holds exactly one entry and it is a directory named like the dataset,
    # use its contents; otherwise use staging's contents as-is.
    local src="$staging"
    local entries=( "$staging"/* )
    if [ "${#entries[@]}" -eq 0 ]; then
        echo "[$name] ERROR: archive extracted to an empty staging dir" >&2
        exit 1
    fi
    if [ "${#entries[@]}" -eq 1 ] && [ -d "${entries[0]}" ] && [ "$(basename "${entries[0]}")" = "$name" ]; then
        src="${entries[0]}"
    fi

    # Publish: wipe any partial payload left by an older run, then move the
    # flattened contents into place.
    rm -rf "$dir"
    mkdir -p "$dir"
    mv "$src"/* "$dir"/
    rm -rf "$staging"

    # Post-check: fail loudly if the archive layout drifted. Shares the
    # single check_payload implementation with the skip-branch verification
    # (round 4).
    if ! check_payload "$dir"; then
        echo "[$name] ERROR: payload verification failed after extraction" >&2
        exit 1
    fi

    # Completion marker: the v2 manifest is written only after every check
    # above passed.
    write_done_manifest "$dir" "$archive_sha" "$done_marker"
    echo "[$name] ok"
}

if [ $# -ne 1 ]; then
    echo "usage: $0 siftsmall|sift|gist|all" >&2
    exit 2
fi

case "$1" in
    siftsmall|sift|gist) fetch_one "$1" ;;
    all) fetch_one siftsmall; fetch_one sift; fetch_one gist ;;
    *) echo "unknown dataset: $1" >&2; exit 2 ;;
esac
