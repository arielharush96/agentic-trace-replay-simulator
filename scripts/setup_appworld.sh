#!/usr/bin/env bash
# Install and initialize AppWorld locally. This is never run by the benchmark.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APPWORLD_ROOT="${APPWORLD_ROOT:?APPWORLD_ROOT must point to a dedicated AppWorld directory}"
VENV="${APPWORLD_VENV:-$ROOT/.venv-appworld}"

mkdir -p "$APPWORLD_ROOT"
python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install --upgrade pip
"$VENV/bin/python" -m pip install appworld
APPWORLD_ROOT="$APPWORLD_ROOT" "$VENV/bin/appworld" install
APPWORLD_ROOT="$APPWORLD_ROOT" "$VENV/bin/appworld" download data

echo "AppWorld initialized at $APPWORLD_ROOT"
echo "Use $VENV/bin/python and APPWORLD_ROOT when starting the adapter."
