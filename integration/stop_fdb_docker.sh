#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${FDB_DOCKER_CONTAINER_NAME:-fdb_integration}"
VOLUME_NAME="${FDB_DOCKER_VOLUME_NAME:-fdb_integration_data}"
REMOVE_VOLUME=false

usage() {
  cat <<EOF
Usage: ./integration/stop_fdb_docker.sh [--delete-volume]

Stops and removes the dedicated host-side FoundationDB integration container.

Options:
  --delete-volume   Also remove the Docker volume ${VOLUME_NAME}.
  -h                Show help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete-volume)
      REMOVE_VOLUME=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
  docker rm -f "${CONTAINER_NAME}" >/dev/null
  echo "Removed container: ${CONTAINER_NAME}"
else
  echo "No container found: ${CONTAINER_NAME}"
fi

if [[ "${REMOVE_VOLUME}" == "true" ]]; then
  if docker volume ls --format '{{.Name}}' | grep -Fxq "${VOLUME_NAME}"; then
    docker volume rm "${VOLUME_NAME}" >/dev/null
    echo "Removed volume: ${VOLUME_NAME}"
  else
    echo "No volume found: ${VOLUME_NAME}"
  fi
fi
