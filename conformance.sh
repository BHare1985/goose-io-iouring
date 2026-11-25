#!/usr/bin/env bash
# Runs the conformance suite against the io_uring event loop, on real sockets: every part of the core and of each capability the loop
# supplies (half-close, sizing, abort, inspection and batching; the other capabilities are not supplied), one process for each part.
# Usage: conformance.sh [--repeat N] [--capability C]
# A part that fails in any repetition fails the run. Unlike the in-memory event loop the real one is not deterministic, so the
# output of repetitions is not compared: a failure in any of them is a failure.
set -u
here="$(cd "$(dirname "$0")" && pwd)"; goose_io="${GOOSE_IO:-${GOOSE_HOME:-$HOME/.goose}/pkg/github.com/BHare1985/goose-io/apply-name-resolution}"; cd "$here"
repeat=1; only=
while [ $# -gt 0 ]; do
    case "$1" in
        --repeat) shift; repeat="${1:?--repeat needs a number}" ;;
        --capability) shift; only="${1:?--capability needs a name}" ;;
        *) echo "unknown argument $1"; exit 2 ;;
    esac
    shift
done
tmp=$(mktemp -d "${TMPDIR:-/tmp}/iouring-conformance.XXXXXX"); trap 'rm -rf "$tmp"' EXIT
./build.sh conformance.goose "$tmp/conformance" >"$tmp/build.log" 2>&1 || { cat "$tmp/build.log"; echo "BUILD FAILED"; exit 1; }
tsv="$goose_io/tests/conformance/checks.tsv"
capabilities=$(grep -v '^#' "$tsv" | cut -f6 | sort -u | grep -vxE 'unix|relay|peek|revoke|wake|limits')
[ -n "$only" ] && capabilities="$only"
# The two addresses "localhost" has, in the order getaddrinfo gives them, for the name-resolution checks of the order.
# PAIR_NAME names another one if "localhost" has one address on this host (it has two where /etc/hosts lists ::1 for it).
pair_name="${PAIR_NAME:-localhost}"
pair=$(getent ahosts "$pair_name" 2>/dev/null | grep STREAM | sed "s/^//" | awk '$2=="STREAM" {print $1}' | head -n 2 | awk '{ if ($1 ~ /:/) printf "[%s]:80 ", $1; else printf "%s:80 ", $1 }')
[ "$(echo $pair | wc -w)" -eq 2 ] || { pair=; echo "note: $pair_name has no two addresses here, so resolve_address_order cannot pass (set PAIR_NAME to a name that has two)"; }
failed=0; total=0
for g in $capabilities; do
    parts=$(grep -v '^#' "$tsv" | awk -F'\t' -v g="$g" '$6 == g {print $2}' | sort -un)
    for ((i = 1; i <= repeat; i++)); do
        for p in $parts; do
            total=$((total + 1))
            "$tmp/conformance" "$p" "$g" ${pair:+$pair_name $pair} >"$tmp/out" 2>&1; rc=$?
            if [ $rc -ne 0 ] && [ -z "$pair" ] && [ "$g" = name-resolution ] && [ "$(grep -c '^ *FAIL' "$tmp/out")" -eq 1 ] && grep -q 'FAIL resolve_address_order' "$tmp/out"; then
                rc=0                                       # the one check that needs a name with two addresses, which this host lacks
            fi
            if [ $rc -ne 0 ]; then
                echo "FAIL  $g part $p, repetition $i"; sed 's/^/      /' "$tmp/out"; failed=$((failed + 1))
            elif [ $i -eq 1 ]; then
                echo "ok    $g part $p: $(tail -n 1 "$tmp/out")"
            fi
        done
    done
done
echo "$total part runs ($repeat repetitions), $failed failed"
[ $failed -eq 0 ]
