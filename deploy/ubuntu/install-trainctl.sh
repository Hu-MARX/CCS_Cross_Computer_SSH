#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR=""
STORE_DIR="/srv/ml-orchestrator"
VENV_DIR=""
CCS_PYTHON="python3"

usage() {
  echo 'Usage: install-trainctl.sh --source CHECKOUT [--python PYTHON] [--store-dir ABSOLUTE] [--venv-dir ABSOLUTE]'
  echo 'Installs the existing source checkout into a new, isolated virtual environment. No service or wrapper is installed.'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --source|--python|--store-dir|--venv-dir)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "$1 requires a value" >&2; exit 2
      fi
      case "$1" in
        --source) SOURCE_DIR="$2" ;;
        --python) CCS_PYTHON="$2" ;;
        --store-dir) STORE_DIR="$2" ;;
        --venv-dir) VENV_DIR="$2" ;;
      esac
      shift 2 ;;
    --install-dir)
      echo '--install-dir is not supported: --source is installed in place. Use --venv-dir to select the isolated environment.' >&2
      exit 2 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DIR" || ! -f "$SOURCE_DIR/pyproject.toml" ]]; then
  echo '--source must point to the existing CCS checkout' >&2
  exit 2
fi
SOURCE_DIR="$(cd -- "$SOURCE_DIR" && pwd -P)"
VENV_DIR="${VENV_DIR:-$STORE_DIR/venv}"
if [[ "$STORE_DIR" != /* || "$VENV_DIR" != /* ]]; then
  echo '--store-dir and --venv-dir must be absolute paths' >&2
  exit 2
fi

# All preflight checks precede creating state directories or an environment.
"$CCS_PYTHON" -c 'import sys; sys.exit("CCS requires Python >=3.11; select a compatible interpreter with --python") if sys.version_info < (3, 11) else None'
if [[ -e "$VENV_DIR" || -L "$VENV_DIR" ]]; then
  echo "Virtual environment already exists: $VENV_DIR. Choose a new --venv-dir; use pip in an existing environment for an explicit upgrade." >&2
  exit 2
fi

umask 077
mkdir -p -- "$STORE_DIR" "$(dirname -- "$VENV_DIR")"
"$CCS_PYTHON" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --no-deps --disable-pip-version-check -e "$SOURCE_DIR"
echo "trainctl executable: $VENV_DIR/bin/trainctl"
echo "Worker state root: $STORE_DIR"
echo 'Keep the source checkout at its current location; this is an editable installation.'
