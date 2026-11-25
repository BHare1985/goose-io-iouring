#!/usr/bin/env bash
# Builds a program that uses the io_uring event loop. `goose run` cannot link the loop's C layer, so programs are built.
# Usage: build.sh <program.goose> <output> [goose build options]
set -eu
src="${1:?usage: build.sh program.goose output [options]}"; out="${2:?usage: build.sh program.goose output [options]}"; shift 2
exec goose build "$src" -o "$out" "$@"
