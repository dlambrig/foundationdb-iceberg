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

assert_body_contains() {
  local name="$1"
  local response="$2"
  local pattern="$3"
  local body
  body="$(http_body "${response}")"
  if ! grep -Eq "${pattern}" <<<"${body}"; then
    echo "Body assertion failed for ${name}. Expected pattern: ${pattern}" >&2
    echo "Body:" >&2
    echo "${body}" >&2
    exit 1
  fi
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
VIEW="orders_view_${RUN_ID}"

echo "==> Bootstrap namespace/table"
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

echo "==> Metrics endpoint check"
METRICS_PAYLOAD='{"report-type":"commit-report","report":{"table-name":"'"${TABLE}"'","snapshot-id":123}}'
METRICS_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${TABLE}/metrics" "${METRICS_PAYLOAD}")"
assert_http_status "report metrics pre-restart" "${METRICS_RESP}" "204"

echo "==> View bootstrap"
CREATE_VIEW_PAYLOAD='{"name":"'"${VIEW}"'","view-version":{"version-id":1,"timestamp-ms":1773000000100,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]},"properties":{"owner":"integration"}}'
CREATE_VIEW="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/views" "${CREATE_VIEW_PAYLOAD}")"
assert_http_status "create view" "${CREATE_VIEW}" "200"
VIEW_METADATA_BEFORE="$(extract_json_field "$(http_body "${CREATE_VIEW}")" "metadata-location")"
if [[ -z "${VIEW_METADATA_BEFORE}" ]]; then
  echo "Failed to extract view metadata-location before restart" >&2
  exit 1
fi

echo "==> Pagination checks"
for child in a b c; do
  CHILD_NS_PAYLOAD="{\"namespace\":[\"${NS}\",\"${child}\"],\"properties\":{}}"
  CHILD_NS_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces" "${CHILD_NS_PAYLOAD}")"
  assert_http_status "create child namespace ${child}" "${CHILD_NS_RESP}" "200"
done

NS_PAGE1="$(run_http_request "GET" "http://localhost:8181/v1/namespaces?parent=${NS}&page-size=2")"
assert_http_status "namespace pagination page1" "${NS_PAGE1}" "200"
assert_body_contains "namespace pagination page1 token" "${NS_PAGE1}" '"next-page-token"'
NS_TOKEN="$(extract_json_field "$(http_body "${NS_PAGE1}")" "next-page-token")"
if [[ -z "${NS_TOKEN}" ]]; then
  echo "Failed to extract namespace next-page-token" >&2
  exit 1
fi
NS_PAGE2="$(run_http_request "GET" "http://localhost:8181/v1/namespaces?parent=${NS}&page-size=2&page-token=${NS_TOKEN}")"
assert_http_status "namespace pagination page2" "${NS_PAGE2}" "200"
assert_body_contains "namespace pagination page2 contains c" "${NS_PAGE2}" "\"${NS}\",\"c\""

for t in t1 t2 t3; do
  TABLE_PAYLOAD="{\"name\":\"${t}\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}"
  TABLE_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables" "${TABLE_PAYLOAD}")"
  assert_http_status "create table ${t}" "${TABLE_RESP}" "200"
done
TABLE_PAGE1="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/tables?page-size=2")"
assert_http_status "table pagination page1" "${TABLE_PAGE1}" "200"
assert_body_contains "table pagination page1 token" "${TABLE_PAGE1}" '"next-page-token"'
TABLE_TOKEN="$(extract_json_field "$(http_body "${TABLE_PAGE1}")" "next-page-token")"
if [[ -z "${TABLE_TOKEN}" ]]; then
  echo "Failed to extract table next-page-token" >&2
  exit 1
fi
TABLE_PAGE2="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/tables?page-size=2&page-token=${TABLE_TOKEN}")"
assert_http_status "table pagination page2" "${TABLE_PAGE2}" "200"
assert_body_contains "table pagination page2" "${TABLE_PAGE2}" '"identifiers"'

for v in v1 v2 v3; do
  VIEW_PAYLOAD='{"name":"'"${v}"'","view-version":{"version-id":1,"timestamp-ms":1773000000200,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}}'
  VIEW_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/views" "${VIEW_PAYLOAD}")"
  assert_http_status "create view ${v}" "${VIEW_RESP}" "200"
done
VIEW_PAGE1="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/views?page-size=2")"
assert_http_status "view pagination page1" "${VIEW_PAGE1}" "200"
assert_body_contains "view pagination page1 token" "${VIEW_PAGE1}" '"next-page-token"'
VIEW_TOKEN="$(extract_json_field "$(http_body "${VIEW_PAGE1}")" "next-page-token")"
if [[ -z "${VIEW_TOKEN}" ]]; then
  echo "Failed to extract view next-page-token" >&2
  exit 1
fi
VIEW_PAGE2="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/views?page-size=2&page-token=${VIEW_TOKEN}")"
assert_http_status "view pagination page2" "${VIEW_PAGE2}" "200"
assert_body_contains "view pagination page2" "${VIEW_PAGE2}" '"identifiers"'

if [[ "${START_SERVER}" == "true" ]]; then
  echo "==> Restart/reload checks (table, view, metrics path)"
  echo "Stopping server for reload checks..."
  stop_server
  require_port_free 8181 "Server"

  echo "Restarting server..."
  start_server

  GET_TABLE_AFTER="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/tables/${TABLE}")"
  assert_http_status "get table after restart" "${GET_TABLE_AFTER}" "200"
  METADATA_AFTER="$(extract_json_field "$(http_body "${GET_TABLE_AFTER}")" "metadata-location")"
  if [[ "${METADATA_BEFORE}" != "${METADATA_AFTER}" ]]; then
    echo "Reload check failed: table metadata-location changed across restart" >&2
    echo "Before: ${METADATA_BEFORE}" >&2
    echo "After:  ${METADATA_AFTER}" >&2
    exit 1
  fi

  GET_VIEW_AFTER="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/views/${VIEW}")"
  assert_http_status "get view after restart" "${GET_VIEW_AFTER}" "200"
  VIEW_METADATA_AFTER="$(extract_json_field "$(http_body "${GET_VIEW_AFTER}")" "metadata-location")"
  if [[ "${VIEW_METADATA_BEFORE}" != "${VIEW_METADATA_AFTER}" ]]; then
    echo "Reload check failed: view metadata-location changed across restart" >&2
    echo "Before: ${VIEW_METADATA_BEFORE}" >&2
    echo "After:  ${VIEW_METADATA_AFTER}" >&2
    exit 1
  fi

  METRICS_RESP_AFTER="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${TABLE}/metrics" "${METRICS_PAYLOAD}")"
  assert_http_status "report metrics post-restart" "${METRICS_RESP_AFTER}" "204"
else
  echo "Skipping restart/reload checks because --no-start-server was set."
fi

echo "==> Repeated concurrent writer conflict checks"
for i in 1 2 3 4 5; do
  CW_TABLE="cw_${RUN_ID}_${i}"
  CW_CREATE_PAYLOAD="{\"name\":\"${CW_TABLE}\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}"
  CW_CREATE_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables" "${CW_CREATE_PAYLOAD}")"
  assert_http_status "create concurrent table ${i}" "${CW_CREATE_RESP}" "200"

  SNAP_ID=$((700000 + i))
  COMMIT_PAYLOAD='{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":'"${SNAP_ID}"',"timestamp-ms":1773000000'"${i}"',"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":'"${SNAP_ID}"',"type":"branch"}]}'

  R1_FILE="$(mktemp)"
  R2_FILE="$(mktemp)"
  (
    run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${CW_TABLE}" "${COMMIT_PAYLOAD}" >"${R1_FILE}"
  ) &
  PID1=$!
  (
    run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${CW_TABLE}" "${COMMIT_PAYLOAD}" >"${R2_FILE}"
  ) &
  PID2=$!

  wait "${PID1}"
  wait "${PID2}"

  STATUS1="$(http_status "$(cat "${R1_FILE}")")"
  STATUS2="$(http_status "$(cat "${R2_FILE}")")"
  rm -f "${R1_FILE}" "${R2_FILE}"

  if ! { [[ "${STATUS1}" == "200" && "${STATUS2}" == "409" ]] || [[ "${STATUS1}" == "409" && "${STATUS2}" == "200" ]]; }; then
    echo "Concurrent writer check iteration ${i} failed: expected one 200 and one 409, got ${STATUS1} and ${STATUS2}" >&2
    exit 1
  fi
done

echo
echo "FDB integration checks passed."
if [[ -n "${SERVER_LOG}" ]]; then
  echo "Server log: ${SERVER_LOG}"
fi
