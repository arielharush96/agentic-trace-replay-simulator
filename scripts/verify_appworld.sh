#!/usr/bin/env bash
set -euo pipefail

APPWORLD_ROOT="${APPWORLD_ROOT:?APPWORLD_ROOT must point to the AppWorld directory}"
VENV="${APPWORLD_VENV:-$(cd "$(dirname "$0")/.." && pwd)/.venv-appworld}"

APPWORLD_ROOT="$APPWORLD_ROOT" "$VENV/bin/appworld" verify tests
APPWORLD_ROOT="$APPWORLD_ROOT" "$VENV/bin/appworld" verify tasks
