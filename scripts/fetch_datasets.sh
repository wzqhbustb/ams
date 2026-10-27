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
#       is unreachable from here (2026-09-03). siftsmall's digest is pinned
#       in this script and is deliberately NOT overridable via environment —
#       it is the recall gate's defense line and must not be bypassable.
#   M4_DATASET_ALLOW_UNPINNED_SIFT / M4_DATASET_ALLOW_UNPINNED_GIST — set to
#       1 to fetch/skip the dataset WITHOUT a pinned digest (2026-09-08
#       Stage E audit round 8). Threat-model conclusion: with no pinned
#       digest there is no external integrity anchor, so a self-consistent
#       forged archive+payload+.done triple is undetectable IN PRINCIPLE
#       (TOFU — the manifest itself may be attacker-written). The script
#       therefore REFUSES unpinned datasets unless this opt-in is set, and
#       prints a loud UNPINNED WARNING on every run (skip included) when it
#       is. Pinning a digest is always preferred.
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
#     the payload still matches the manifest file-for-file (round-5 note)
#     AND the three harness file classes still exist non-empty (round-6
#     note);
#   - archive members are validated BEFORE extraction: regular files and
#     directories only, no absolute names, no .. components (2026-09-08
#     Stage E audit round 6 — a top-level symlink member could move
#     arbitrary files into the dataset dir);
#   - archives are size-capped per dataset and gzip-tested before use; the
#     download stream itself is hard-capped (curl --max-filesize + head -c
#     truncation, round 7); an archive that fails validation is deleted and
#     re-downloaded, never silently reused (2026-09-08 Stage E audit);
#   - extraction is bounded too: per-member + total extracted-size ceilings,
#     and the published payload must be EXACTLY the expected texmex file set
#     (2026-09-08 Stage E audit round 7);
#   - the skip path's integrity anchor is the VERIFIED ARCHIVE, not .done:
#     skip re-verifies the archive sha256 and re-derives payload hashes by
#     streaming archive members (round 7 — a self-signed manifest is
#     worthless against this);
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
#
# AUTHORITY NOTE (2026-09-08, Stage E audit round 7, finding 1): .done is a
# COMPLETION MARKER, not an integrity anchor — anyone can write a
# structurally valid manifest matching a planted payload (the audit did:
# garbage query file + self-signed hashes -> "payload verified — skipping").
# The payload-hash lines above are retained as a REDUNDANT QUICK PATH
# (file-set + manifest-vs-disk comparison); the AUTHORITATIVE content check
# on the skip path is verify_payload_from_archive(), which streams member
# hashes out of the VERIFIED ARCHIVE and compares them against the disk.

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
    # Structure check first (2026-09-08 Stage E audit round 6): a sparse
    # manifest — magic + archive line but NO payload lines — paired with an
    # empty payload dir previously passed the set comparison below and the
    # script skipped with an EMPTY payload. Require at least one payload
    # line, every line well-formed: 3 fields, 64-char lowercase-hex sha256
    # (length+charset instead of {64} intervals — BSD awk portability),
    # numeric byte size, non-empty name.
    local malformed
    malformed="$(tail -n +3 "$manifest" | awk 'NF!=3 || length($1)!=64 || $1 !~ /^[0-9a-f]+$/ || $2 !~ /^[0-9]+$/ || $3=="" { print NR": "$0 }')"
    if [ -z "$(tail -n +3 "$manifest")" ]; then
        echo ".done manifest has no payload lines — structurally invalid" >&2
        return 1
    fi
    if [ -n "$malformed" ]; then
        echo ".done manifest has malformed payload line(s):" >&2
        echo "$malformed" >&2
        return 1
    fi
    # File-set comparison: missing AND extra files both fail.
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

# 2026-09-08 Stage E audit round 6 (finding 3): sanity ceiling on the
# ARCHIVE size per dataset, several times the expected .tar.gz size. The
# sift/gist fetches are unpinned ftp downloads — without a ceiling a broken
# or hostile endpoint could fill the disk unboundedly.
max_archive_bytes() {
    case "$1" in
        siftsmall) echo $((16 * 1024 * 1024)) ;;    # actual ~5.3 MB
        sift)      echo $((512 * 1024 * 1024)) ;;   # actual ~160 MB
        gist)      echo $((4096 * 1024 * 1024)) ;;  # extracted ~3.8 GB; compressed well under
        *) return 1 ;;
    esac
}

# 2026-09-08 Stage E audit round 7 (finding 3a): sanity ceiling on the
# EXTRACTED size per dataset (per-member AND total), several times the real
# unpacked corpus. Basis: siftsmall unpacks to ~17.8 MB; sift to ~560 MB
# (1M x 128 f32 base + learn/query/gt); gist to ~5.8 GB (1M x 960 f32 base
# + 500k learn). Prevents an archive from smuggling oversized members even
# when its compressed size stays under the archive ceiling.
max_extracted_bytes() {
    case "$1" in
        siftsmall) echo $((32 * 1024 * 1024)) ;;       # actual ~17.8 MB
        sift)      echo $((1024 * 1024 * 1024)) ;;     # actual ~560 MB
        gist)      echo $((8 * 1024 * 1024 * 1024)) ;; # actual ~5.8 GB
        *) return 1 ;;
    esac
}

# The exact texmex corpus file set per dataset — used by the publish gate
# (2026-09-08 Stage E audit round 7, finding 3b): no more, no less.
expected_payload_files() {
    case "$1" in
        siftsmall|sift|gist)
            echo "$1_base.fvecs $1_groundtruth.ivecs $1_learn.fvecs $1_query.fvecs" ;;
        *) return 1 ;;
    esac
}

# Cheap archive plausibility gate, applied to EVERY archive before it is
# used (fresh download or reuse): size under the per-dataset ceiling and
# gzip integrity OK. Prints the failure reason to stderr, returns 1.
archive_plausible() {
    local archive="$1" max_bytes="$2"
    local size
    size="$(byte_size "$archive")"
    if [ "$size" -gt "$max_bytes" ]; then
        echo "$archive is ${size}B, over the sanity ceiling of ${max_bytes}B for this dataset" >&2
        return 1
    fi
    # gzip -t decompresses fully to verify integrity (gist-scale: seconds).
    if ! gzip -t "$archive" 2>/dev/null; then
        echo "$archive failed 'gzip -t' (truncated or corrupt)" >&2
        return 1
    fi
    return 0
}

# 2026-09-08 Stage E audit round 6 (finding 1): validate tar members BEFORE
# extraction. Reproduce chain of the bug: a tar whose single top-level entry
# is a symlink named like the dataset passed the flatten heuristic
# ([ -d ] follows symlinks), and 'mv "$src"/*' then MOVED the symlink
# target's files into the dataset dir — arbitrary file relocation, followed
# by "ok". Two portable checks (bsdtar/macOS and GNU tar both put the member
# type flag in column 1 of -tv output and print plain names with -t):
#   - member types: regular files (-) and directories (d) only; symlinks
#     (l), hardlinks (h), char/block devices (c/b), fifos (p) are rejected;
#   - member names: no absolute paths (^/), no .. path components.
verify_archive_members() {
    local archive="$1"
    local bad
    bad="$(tar -tvzf "$archive" | awk '{t=substr($0,1,1); if (t!="-" && t!="d") print}')"
    if [ -n "$bad" ]; then
        echo "archive contains non-regular/non-directory members (symlink/hardlink/device/fifo) — refusing to extract:" >&2
        echo "$bad" >&2
        return 1
    fi
    bad="$(tar -tzf "$archive" | grep -E '^/|(^|/)\.\.(/|$)' || true)"
    if [ -n "$bad" ]; then
        echo "archive contains path-traversal members (absolute or .. names) — refusing to extract:" >&2
        echo "$bad" >&2
        return 1
    fi
    return 0
}

# 2026-09-08 Stage E audit round 7 (finding 3a): per-member and total
# EXTRACTED size ceilings. Streaming each member through wc -c (tar -xzO is
# portable across bsdtar/GNU tar; parsing the -tv size column is NOT — the
# column offset differs between the two). Cost: one extra decompression
# pass of the archive before extraction; siftsmall-scale this is
# milliseconds, gist-scale seconds — bounded, and only on the fetch path.
verify_archive_sizes() {
    local archive="$1" max_extracted="$2"
    local member size total=0
    while read -r member; do
        case "$member" in */) continue ;; esac   # directory entries
        size="$(tar -xzOf "$archive" "$member" | wc -c | tr -d ' ')"
        if [ "$size" -gt "$max_extracted" ]; then
            echo "archive member '$member' is ${size}B, over the per-member ceiling of ${max_extracted}B" >&2
            return 1
        fi
        total=$((total + size))
        if [ "$total" -gt "$max_extracted" ]; then
            echo "archive members total over ${max_extracted}B extracted (at '$member') — refusing" >&2
            return 1
        fi
    done < <(tar -tzf "$archive")
    return 0
}

# 2026-09-08 Stage E audit round 7 (finding 1): the ARCHIVE is the
# integrity anchor on the skip path. Its sha256 must match the pinned/env
# expectation when one exists, else the manifest's recorded value (proof
# the archive is unchanged since it produced the payload). A self-signed
# .done cannot forge this: the attacker would have to forge the pinned
# archive itself.
verify_archive_anchor() {
    local archive="$1" expected="$2" recorded="$3"
    local actual
    actual="$(sha256_of "$archive")"
    if [ -n "$expected" ]; then
        if [ "$actual" != "$expected" ]; then
            echo "archive sha256 $actual != expected $expected" >&2
            return 1
        fi
    elif [ "$actual" != "$recorded" ]; then
        echo "archive sha256 $actual != manifest-recorded $recorded" >&2
        return 1
    fi
    return 0
}

# 2026-09-08 Stage E audit round 7 (finding 1): AUTHORITATIVE payload
# content check on the skip path — stream each archive member's sha256
# straight out of the verified archive (tar -xzO | shasum, no extraction to
# disk) and compare against the on-disk payload file. Member-name -> disk
# -file mapping follows the existing flatten rule: strip a leading ./ and
# the top-level "<dataset>/" component. Cost: one decompression pass per
# member (siftsmall ~5MB: milliseconds; gist ~1.5GB x 4 members: seconds) —
# skip-path only, and this is the correctness defense line.
verify_payload_from_archive() {
    local archive="$1" dir="$2" name="$3"
    local member flat arch_sha disk_sha
    while read -r member; do
        case "$member" in */) continue ;; esac   # directory entries
        flat="${member#./}"
        case "$flat" in "$name/"*) flat="${flat#"$name"/}" ;; esac
        if [ ! -f "$dir/$flat" ]; then
            echo "$flat: archive member missing on disk under $dir" >&2
            return 1
        fi
        arch_sha="$(tar -xzOf "$archive" "$member" | shasum -a 256 | awk '{print $1}')"
        disk_sha="$(sha256_of "$dir/$flat")"
        if [ "$arch_sha" != "$disk_sha" ]; then
            echo "$dir/$flat does not match the verified archive (archive $arch_sha, disk $disk_sha)" >&2
            return 1
        fi
    done < <(tar -tzf "$archive")
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

    # 2026-09-08 Stage E audit round 8 (finding 2): with NO pinned digest
    # there is no external integrity anchor at all — a self-consistent
    # forged triple (archive + payload + .done) passes every check including
    # the round-7 archive anchor, because the "anchor" value itself comes
    # from the potentially forged manifest. This is TOFU and cannot be
    # detected in principle, so the fix is to eliminate the SILENCE: an
    # unpinned dataset requires an explicit per-dataset opt-in
    # (M4_DATASET_ALLOW_UNPINNED_<NAME>=1) for BOTH download and skip, and
    # even then warns loudly on EVERY run. siftsmall carries a built-in pin
    # and never reaches this gate.
    if [ -z "$expected" ]; then
        local allow_var="M4_DATASET_ALLOW_UNPINNED_$upper"
        if [ "${!allow_var:-}" != "1" ]; then
            echo "[$name] ERROR: no pinned SHA-256 for $name — without one there is NO external integrity anchor (a forged archive+payload+.done triple is undetectable; Stage E round 8)" >&2
            echo "[$name]   preferred: pin one via M4_DATASET_SHA256_$upper=<sha256-of-a-verified-copy>" >&2
            echo "[$name]   or explicitly accept the forgery risk: $allow_var=1" >&2
            exit 1
        fi
        echo "[$name] UNPINNED WARNING: no external integrity anchor for $name — a self-consistent forged archive+payload+.done triple is UNDETECTABLE; proceeding only because $allow_var=1 was set explicitly (2026-09-08 Stage E round 8)" >&2
    fi

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
    # (siftsmall ~17MB — milliseconds; gist ~4GB — seconds) and (round 7)
    # additionally streams the archive once per member. It runs only on the
    # skip-decision path; on CI a cache hit means this fetch step runs
    # anyway, and this is the correctness defense line.
    #
    # 2026-09-08 Stage E audit round 7 (finding 1): round 5's manifest
    # comparison trusts .done as the hash SOURCE — but anyone can write a
    # structurally valid manifest matching a planted payload (the audit
    # self-signed one around a garbage query file and the script reported
    # "payload verified — skipping"). The integrity anchor is therefore
    # moved to the VERIFIED ARCHIVE: skip now requires the archive present,
    # its sha256 matching the anchor (pinned/env expectation, else the
    # manifest's recorded value), and every payload file matching the hash
    # streamed from the archive (verify_payload_from_archive). If the
    # archive is gone, .done alone cannot prove anything -> INCOMPLETE,
    # re-download. The manifest payload lines stay as a redundant quick
    # path (see the AUTHORITY NOTE at the format definition).
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
        elif [ ! -s "$archive" ]; then
            # Round 7: no archive -> no anchor -> .done is untrusted.
            echo "[$name] INCOMPLETE: .done present but the archive is gone — a manifest alone is not an integrity anchor (round 7)" >&2
            echo "[$name] wiping payload dir and .done; re-downloading" >&2
            rm -rf "$dir"
        # Round 6 (2026-09-08 Stage E audit, finding 2): manifest
        # verification alone trusted a sparse manifest + empty dir, so
        # check_payload (the three harness file classes, exist + non-empty)
        # runs again on the actual payload before any skip.
        elif ! verify_done_manifest "$dir" "$done_marker" || ! check_payload "$dir"; then
            echo "[$name] INCOMPLETE: .done present but payload verification failed (reason above)" >&2
            echo "[$name] wiping payload dir and .done; re-extracting from the verified archive (kept)" >&2
            rm -rf "$dir"
        elif ! verify_archive_anchor "$archive" "$expected" "$recorded"; then
            # Round 7: the archive itself is tampered/mismatched — it can no
            # longer anchor anything; wipe it too and re-download.
            echo "[$name] STALE: archive failed the anchor check (reason above)" >&2
            echo "[$name] wiping payload dir, .done and archive; re-downloading" >&2
            rm -rf "$dir" "$archive"
        elif ! verify_payload_from_archive "$archive" "$dir" "$name"; then
            # Round 7: payload diverges from the VERIFIED ARCHIVE — this is
            # the authoritative content check; the archive is kept.
            echo "[$name] INCOMPLETE: payload diverges from the verified archive (reason above)" >&2
            echo "[$name] wiping payload dir and .done; re-extracting from the verified archive (kept)" >&2
            rm -rf "$dir"
        else
            echo "[$name] .done manifest present (archive sha256: $recorded), payload verified against the archive — skipping"
            return 0
        fi
    fi

    mkdir -p "$DATASETS_DIR"

    # Clean up leftovers from an interrupted previous run before doing
    # anything else.
    rm -rf "$staging" "$part"

    # 2026-09-08 Stage E audit round 6 (finding 3): an archive that fails
    # the plausibility gate (size ceiling / gzip -t) must never be silently
    # reused — a corrupt archive previously survived a failed run and was
    # "reused" forever after. Reuse is now conditional on validation; a
    # failing archive is deleted and re-downloaded (self-heal). For
    # unpinned datasets (sift/gist without M4_DATASET_SHA256_*) this gate is
    # the ONLY integrity check, so it is unconditional; the pinned-sha check
    # below remains the strong one.
    local max_bytes
    max_bytes="$(max_archive_bytes "$name")"
    if [ -s "$archive" ]; then
        if archive_plausible "$archive" "$max_bytes"; then
            echo "[$name] reusing existing archive $archive ($(byte_size "$archive") bytes)"
        else
            echo "[$name] existing archive failed validation (reason above) — deleting and re-downloading" >&2
            rm -f "$archive"
        fi
    fi
    if [ ! -s "$archive" ]; then
        echo "[$name] downloading $url"
        # curl handles both ftp:// and http(s)://; -f fails loudly on HTTP
        # errors, -L follows redirects (self-hosted release scenario).
        #
        # 2026-09-08 Stage E audit round 7 (finding 2): enforce the size
        # ceiling ON THE STREAM — previously a 17.8MB body downloaded in
        # full before the 16MiB ceiling rejected it. Two layers:
        #   - curl --max-filesize refuses up front when the server reports a
        #     Content-Length (covers http(s) with proper headers and
        #     file://); a server without Content-Length bypasses it, hence:
        #   - the body is piped through head -c (ceiling+1): the stream is
        #     hard-truncated at ceiling+1 bytes, so the .part on disk can
        #     never exceed that. Afterwards, .part larger than the ceiling
        #     means overflow -> reject. curl exiting 23 (SIGPIPE write
        #     error) under truncation is the EXPECTED path, distinguished
        #     from real failures by checking the .part size FIRST.
        # Download to .part first; the mv publishes the archive atomically
        # once the download completed AND passed the plausibility gate. Any
        # failure removes the .part residue so no later run can pick it up.
        local cap=$((max_bytes + 1)) curl_rc
        set +e
        curl -fSL --retry 3 --max-filesize "$max_bytes" "$url" | head -c "$cap" > "$part"
        curl_rc=${PIPESTATUS[0]}
        set -e
        if [ -f "$part" ] && [ "$(byte_size "$part")" -gt "$max_bytes" ]; then
            echo "[$name] ERROR: download exceeded the sanity ceiling of ${max_bytes}B (stream truncated, rejecting)" >&2
            rm -f "$part"
            exit 1
        fi
        if [ "$curl_rc" -eq 63 ]; then
            echo "[$name] ERROR: server-reported size exceeds the sanity ceiling of ${max_bytes}B (curl --max-filesize)" >&2
            rm -f "$part"
            exit 1
        fi
        if [ "$curl_rc" -ne 0 ]; then
            echo "[$name] ERROR: download failed from $url (curl exit $curl_rc)" >&2
            rm -f "$part"
            exit 1
        fi
        if ! archive_plausible "$part" "$max_bytes"; then
            echo "[$name] ERROR: downloaded archive failed validation (reason above)" >&2
            rm -f "$part"
            exit 1
        fi
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
    # Round 6 (finding 1): validate member types and names BEFORE untarring
    # — a symlink/hardlink/device member or a .. /absolute name must be
    # rejected here, not after it moved files around.
    # Round 9: a rejected archive must NOT survive — for unpinned datasets
    # there is no SHA line of defense above, and archive_plausible() does not
    # check members, so a kept bad archive would pass the reuse gate forever:
    # switching M4_DATASET_URL_<NAME> to a good source would never download
    # (permanent brick, external audit round 3 residual ② — the v1.24
    # "self-heal, no loop" claim held only for the size-cap and SHA paths).
    if ! verify_archive_members "$archive"; then
        echo "[$name] ERROR: archive member validation failed for $archive" >&2
        echo "[$name] deleting the archive; re-run to fetch a fresh copy" >&2
        rm -rf "$staging"
        rm -f "$archive"
        exit 1
    fi
    # Round 7 (finding 3a): per-member + total extracted-size ceilings.
    if ! verify_archive_sizes "$archive" "$(max_extracted_bytes "$name")"; then
        echo "[$name] ERROR: archive size validation failed for $archive" >&2
        echo "[$name] deleting the archive; re-run to fetch a fresh copy" >&2
        rm -rf "$staging"
        rm -f "$archive"
        exit 1
    fi
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

    # Round 7 (finding 3b): publish exactly the expected texmex file set —
    # no more, no less. check_payload below only proves the EXPECTED files
    # exist non-empty; an archive smuggling an extra member (the audit used
    # a 64MiB pad) previously sailed through staging into the payload.
    local want have
    want="$(expected_payload_files "$name" | tr ' ' '\n' | sort)"
    have="$(ls -A "$src" | sort)"
    if [ "$want" != "$have" ]; then
        echo "[$name] ERROR: extracted file set under $src differs from the expected texmex corpus files:" >&2
        diff <(echo "$want") <(echo "$have") >&2 || true
        rm -rf "$staging"
        exit 1
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
