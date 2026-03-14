#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/fdb_server_${RUN_ID}.log"
SERVER_GRADLE_ARGS=(-Dfdb=true)

START_SERVER=true
RUN_SMOKE=true

usage() {
  cat <<'EOF'
Usage: ./integration/run_fdb_integration.sh [--no-smoke] [--no-start-server]

Options:
  --no-smoke         Skip ./integration/trino_smoke.sh --fdb pre-check.
  --no-start-server  Use an already-running server on localhost:8181 for REST checks.
  -h                 Show help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-smoke)
      RUN_SMOKE=false
      shift
      ;;
    --no-start-server)
      START_SERVER=false
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

mkdir -p "${LOG_DIR}"

SERVER_PID=""

cleanup() {
  set +e
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_port_free() {
  local port="$1"
  local name="$2"
  local pids
  pids="$(lsof -ti tcp:"${port}" || true)"
  if [[ -n "${pids}" ]]; then
    echo "ERROR: ${name} port ${port} is already in use by PID(s): ${pids}" >&2
    echo "Stop existing process(es) first, or run: kill ${pids}" >&2
    exit 1
  fi
}

kill_port_processes() {
  local port="$1"
  local pids
  pids="$(lsof -ti tcp:"${port}" || true)"
  if [[ -n "${pids}" ]]; then
    kill ${pids} >/dev/null 2>&1 || true
    sleep 1
    pids="$(lsof -ti tcp:"${port}" || true)"
    if [[ -n "${pids}" ]]; then
      kill -9 ${pids} >/dev/null 2>&1 || true
    fi
  fi
}

wait_for_http() {
  local url="$1"
  local name="$2"
  for _ in {1..120}; do
    if curl -sf "${url}" >/dev/null 2>&1; then
      echo "${name} is ready"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Timed out waiting for ${name}" >&2
  return 1
}

require_fdb_prereqs() {
  local cluster_file="${PROJECT_ROOT}/fdb.cluster"
  if [[ ! -f "${cluster_file}" ]]; then
    echo "ERROR: FDB integration requires ${cluster_file}" >&2
    exit 1
  fi

  local fdb_lib="${FDB_LIBRARY_PATH_FDB_C:-/usr/local/lib/libfdb_c.dylib}"
  if [[ ! -f "${fdb_lib}" ]]; then
    echo "ERROR: FDB integration requires FoundationDB client library at ${fdb_lib}" >&2
    echo "Set FDB_LIBRARY_PATH_FDB_C to the correct libfdb_c path." >&2
    exit 1
  fi

  if ! command -v fdbcli >/dev/null 2>&1; then
    echo "ERROR: FDB integration requires 'fdbcli' to verify FoundationDB is running." >&2
    exit 1
  fi

  local status_output
  if ! status_output="$(fdbcli -C "${cluster_file}" --exec "status" 2>&1)"; then
    echo "ERROR: FoundationDB is not reachable using ${cluster_file}." >&2
    echo "Start FoundationDB first, then retry ./integration/run_fdb_integration.sh" >&2
    echo "fdbcli output:" >&2
    echo "${status_output}" >&2
    exit 1
  fi
}

run_http_request() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"
  if [[ -n "${payload}" ]]; then
    curl -sS -X "${method}" -H "Content-Type: application/json" \
      -d "${payload}" -w $'\nHTTP_STATUS:%{http_code}' "${url}"
  else
    curl -sS -X "${method}" -w $'\nHTTP_STATUS:%{http_code}' "${url}"
  fi
}

http_status() {
  local response="$1"
  echo "${response##*HTTP_STATUS:}"
}

http_body() {
  local response="$1"
  echo "${response%$'\n'HTTP_STATUS:*}"
}

extract_json_field() {
  local json="$1"
  local field="$2"
  echo "${json}" | sed -nE "s/.*\"${field}\":\"([^\"]+)\".*/\\1/p"
}

assert_http_status() {
  local name="$1"
  local response="$2"
  local expected="$3"
  local actual
  actual="$(http_status "${response}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "HTTP assertion failed for ${name}: expected ${expected}, got ${actual}" >&2
    echo "Body:" >&2
    http_body "${response}" >&2
    exit 1
  fi
}

start_server() {
  if [[ "${START_SERVER}" != "true" ]]; then
    wait_for_http "http://localhost:8181/v1/config" "Server"
    return
  fi
  kill_port_processes 8181
  require_port_free 8181 "Server"
  (
    cd "${PROJECT_ROOT}"
    ./gradlew runIcebergRestServer "${SERVER_GRADLE_ARGS[@]}"
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  wait_for_http "http://localhost:8181/v1/config" "Server"
}

stop_server() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
    SERVER_PID=""
  fi
  kill_port_processes 8181
}

require_fdb_prereqs

if [[ "${RUN_SMOKE}" == "true" ]]; then
  echo "==> Running Trino smoke in FDB mode"
  "${PROJECT_ROOT}/integration/trino_smoke.sh" --fdb
fi

echo "==> Starting FDB-backed server for REST checks"
start_server

NS="fdb_it_${RUN_ID}"
TABLE="orders_${RUN_ID}"

echo "==> Restart/reload check"
CREATE_NS="$(run_http_request "POST" "http://localhost:8181/v1/namespaces" "{\"namespace\":[\"${NS}\"],\"properties\":{}}")"
assert_http_status "create namespace" "${CREATE_NS}" "200"

CREATE_TABLE_PAYLOAD="{\"name\":\"${TABLE}\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}"
CREATE_TABLE="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables" "${CREATE_TABLE_PAYLOAD}")"
assert_http_status "create table" "${CREATE_TABLE}" "200"
TABLE_BODY_BEFORE="$(http_body "${CREATE_TABLE}")"
METADATA_BEFORE="$(extract_json_field "${TABLE_BODY_BEFORE}" "metadata-location")"
if [[ -z "${METADATA_BEFORE}" ]]; then
  echo "Failed to extract metadata-location before restart" >&2
  exit 1
fi

echo "Stopping server for reload check..."
stop_server
require_port_free 8181 "Server"

echo "Restarting server..."
start_server

GET_TABLE_AFTER="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/tables/${TABLE}")"
assert_http_status "get table after restart" "${GET_TABLE_AFTER}" "200"
METADATA_AFTER="$(extract_json_field "$(http_body "${GET_TABLE_AFTER}")" "metadata-location")"
if [[ "${METADATA_BEFORE}" != "${METADATA_AFTER}" ]]; then
  echo "Reload check failed: metadata-location changed across restart" >&2
  echo "Before: ${METADATA_BEFORE}" >&2
  echo "After:  ${METADATA_AFTER}" >&2
  exit 1
fi

echo "==> Concurrent writer conflict check"
COMMIT1_PAYLOAD='{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":700001,"timestamp-ms":1773000000001,"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":700001,"type":"branch"}]}'

R1_FILE="$(mktemp)"
R2_FILE="$(mktemp)"

(
  run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${TABLE}" "${COMMIT1_PAYLOAD}" >"${R1_FILE}"
) &
PID1=$!
(
  run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${TABLE}" "${COMMIT1_PAYLOAD}" >"${R2_FILE}"
) &
PID2=$!

wait "${PID1}"
wait "${PID2}"

STATUS1="$(http_status "$(cat "${R1_FILE}")")"
STATUS2="$(http_status "$(cat "${R2_FILE}")")"
rm -f "${R1_FILE}" "${R2_FILE}"

if ! { [[ "${STATUS1}" == "200" && "${STATUS2}" == "409" ]] || [[ "${STATUS1}" == "409" && "${STATUS2}" == "200" ]]; }; then
  echo "Concurrent writer check failed: expected one 200 and one 409, got ${STATUS1} and ${STATUS2}" >&2
  exit 1
fi

echo
echo "FDB integration checks passed."
if [[ -n "${SERVER_LOG}" ]]; then
  echo "Server log: ${SERVER_LOG}"
fi
