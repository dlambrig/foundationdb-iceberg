#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER_NAME="${FDB_DOCKER_CONTAINER_NAME:-fdb_integration}"
IMAGE="${FDB_DOCKER_IMAGE:-foundationdb/foundationdb:7.3.38}"
PORT="${FDB_DOCKER_PORT:-4550}"
VOLUME_NAME="${FDB_DOCKER_VOLUME_NAME:-fdb_integration_data}"
CLUSTER_FILE="${FDB_CLUSTER_FILE_PATH:-${PROJECT_ROOT}/fdb.cluster}"
PROCESS_CLASS="${FDB_PROCESS_CLASS:-unset}"
STARTUP_SCRIPT="${PROJECT_ROOT}/integration/fdb_docker_entrypoint.sh"
REPLACE=false

usage() {
  cat <<EOF
Usage: ./integration/start_fdb_docker.sh [--replace]

Starts a dedicated FoundationDB container for host-side integration tests.
This is separate from docker-compose and forces the server to advertise
127.0.0.1:${PORT} so host tools like fdbcli and the integration scripts can connect.

Options:
  --replace   Remove and recreate the named container if it already exists.
  -h          Show help.

Environment overrides:
  FDB_DOCKER_CONTAINER_NAME   Container name (default: ${CONTAINER_NAME})
  FDB_DOCKER_IMAGE            Image to run (default: ${IMAGE})
  FDB_DOCKER_PORT             Host/container port (default: ${PORT})
  FDB_DOCKER_VOLUME_NAME      Docker volume for /var/fdb/data (default: ${VOLUME_NAME})
  FDB_CLUSTER_FILE_PATH       Host cluster file to write (default: ${CLUSTER_FILE})
  FDB_PROCESS_CLASS           FoundationDB process class (default: ${PROCESS_CLASS})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --replace)
      REPLACE=true
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

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: Missing required command: $1" >&2
    exit 1
  fi
}

require_command docker
require_command fdbcli

if [[ ! -f "${STARTUP_SCRIPT}" ]]; then
  echo "ERROR: Missing startup script: ${STARTUP_SCRIPT}" >&2
  exit 1
fi

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"
}

container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"
}

write_cluster_file() {
  mkdir -p "$(dirname "${CLUSTER_FILE}")"
  printf 'docker:docker@127.0.0.1:%s\n' "${PORT}" > "${CLUSTER_FILE}"
}

wait_for_fdb() {
  local status_output=""
  local available=false
  local configured=false

  for _ in {1..60}; do
    status_output="$(
      python3 - "${CLUSTER_FILE}" <<'PY'
import subprocess
import sys

cluster_file = sys.argv[1]
try:
    proc = subprocess.run(
        ["fdbcli", "-C", cluster_file, "--exec", "status"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
except subprocess.TimeoutExpired as exc:
    if exc.stdout:
        print(exc.stdout, end="")
    if exc.stderr:
        print(exc.stderr, end="", file=sys.stderr)
    print("FDBCLI_STATUS_TIMEOUT")
PY
    )"
    if grep -q "The database is available." <<<"${status_output}"; then
      available=true
      break
    fi
    if [[ "${configured}" != "true" ]] && ! grep -Eqi "unreachable|could not communicate with a quorum|connection refused|timed out|FDBCLI_STATUS_TIMEOUT" <<<"${status_output}"; then
      fdbcli -C "${CLUSTER_FILE}" --exec "configure new single ssd tenant_mode=optional_experimental" >/dev/null 2>&1 || true
      configured=true
    fi
    sleep 1
  done

  if [[ "${available}" != "true" ]]; then
    echo "ERROR: Timed out waiting for FoundationDB to become available." >&2
    echo "Last fdbcli output:" >&2
    echo "${status_output}" >&2
    echo "Recent container logs:" >&2
    docker logs --tail 80 "${CONTAINER_NAME}" >&2 || true
    exit 1
  fi
}

if container_exists; then
  if [[ "${REPLACE}" == "true" ]]; then
    docker rm -f "${CONTAINER_NAME}" >/dev/null
  elif container_running; then
    write_cluster_file
    wait_for_fdb
    echo "Reusing running FoundationDB container: ${CONTAINER_NAME}"
    echo "Cluster file: ${CLUSTER_FILE}"
    exit 0
  else
    echo "ERROR: Container ${CONTAINER_NAME} already exists but is not running." >&2
    echo "Use --replace to recreate it." >&2
    exit 1
  fi
fi

write_cluster_file

docker volume create "${VOLUME_NAME}" >/dev/null
docker run -d \
  --name "${CONTAINER_NAME}" \
  --entrypoint /bin/bash \
  -e FDB_PORT="${PORT}" \
  -e FDB_PROCESS_CLASS="${PROCESS_CLASS}" \
  -p "${PORT}:${PORT}/tcp" \
  -v "${VOLUME_NAME}:/var/fdb/data" \
  -v "${STARTUP_SCRIPT}:/fdb-start.sh:ro" \
  "${IMAGE}" \
  /fdb-start.sh >/dev/null

wait_for_fdb

echo "FoundationDB container is ready: ${CONTAINER_NAME}"
echo "Cluster file: ${CLUSTER_FILE}"
