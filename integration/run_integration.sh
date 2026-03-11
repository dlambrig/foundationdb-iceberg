#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/server_${RUN_ID}.log"
TRINO_LOG="${LOG_DIR}/trino_${RUN_ID}.log"
SERVER_MODE="memory"
SERVER_GRADLE_ARGS=()

usage() {
  cat <<'EOF'
Usage: ./integration/run_integration.sh [--fdb]

Options:
  --fdb    Run IcebergRestServer in FoundationDB mode (-Dfdb=true).
  -h       Show help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fdb)
      SERVER_MODE="fdb"
      SERVER_GRADLE_ARGS=(-Dfdb=true)
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

TRINO_HOME="${TRINO_HOME:-/Users/dlambrig/trino}"
TRINO_ETC="${TRINO_ETC:-${TRINO_HOME}/etc}"
TRINO_SERVER_URL="${TRINO_SERVER_URL:-http://localhost:8080}"

TRINO_SERVER_DIR="${TRINO_SERVER_DIR:-}"
if [[ -z "${TRINO_SERVER_DIR}" ]]; then
  TRINO_SERVER_DIR="$(ls -d "${TRINO_HOME}"/core/trino-server/target/trino-server-* 2>/dev/null | head -n1 || true)"
fi

TRINO_LAUNCHER="${TRINO_LAUNCHER:-}"
if [[ -z "${TRINO_LAUNCHER}" ]]; then
  if [[ -x "${TRINO_SERVER_DIR}/bin/darwin-amd64/launcher" ]]; then
    TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/darwin-amd64/launcher"
  elif [[ -x "${TRINO_SERVER_DIR}/bin/linux-amd64/launcher" ]]; then
    TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/linux-amd64/launcher"
  elif [[ -x "${TRINO_SERVER_DIR}/bin/launcher" ]]; then
    TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/launcher"
  fi
fi

TRINO_CLI_JAR="${TRINO_CLI_JAR:-}"
if [[ -z "${TRINO_CLI_JAR}" ]]; then
  TRINO_CLI_JAR="$(ls "${TRINO_HOME}"/client/trino-cli/target/trino-cli-*-executable.jar 2>/dev/null | head -n1 || true)"
fi

if [[ -z "${TRINO_SERVER_DIR}" || ! -d "${TRINO_SERVER_DIR}" ]]; then
  echo "ERROR: Unable to find Trino server directory. Set TRINO_SERVER_DIR." >&2
  exit 1
fi

if [[ -z "${TRINO_LAUNCHER}" || ! -x "${TRINO_LAUNCHER}" ]]; then
  echo "ERROR: Unable to find Trino launcher. Set TRINO_LAUNCHER." >&2
  exit 1
fi

if [[ -z "${TRINO_CLI_JAR}" || ! -f "${TRINO_CLI_JAR}" ]]; then
  echo "ERROR: Unable to find trino-cli executable jar. Set TRINO_CLI_JAR." >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"

SERVER_PID=""
TRINO_PID=""

cleanup() {
  set +e
  if [[ -n "${TRINO_PID}" ]]; then
    kill "${TRINO_PID}" >/dev/null 2>&1 || true
    wait "${TRINO_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi

  # Gradle/launcher can leave child processes behind; force-clear integration ports.
  for port in 8181 8080; do
    local pids
    pids="$(lsof -ti tcp:${port} || true)"
    if [[ -n "${pids}" ]]; then
      kill ${pids} >/dev/null 2>&1 || true
      sleep 1
      pids="$(lsof -ti tcp:${port} || true)"
      if [[ -n "${pids}" ]]; then
        kill -9 ${pids} >/dev/null 2>&1 || true
      fi
    fi
  done
}
trap cleanup EXIT

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

run_sql_raw() {
  local sql="$1"
  java -jar "${TRINO_CLI_JAR}" --server "${TRINO_SERVER_URL}" --output-format TSV_HEADER --execute "${sql}"
}

wait_for_trino() {
  for _ in {1..120}; do
    if run_sql_raw "SELECT 1" >/dev/null 2>&1; then
      echo "Trino is ready"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Timed out waiting for Trino" >&2
  return 1
}

require_fdb_prereqs() {
  local cluster_file="${PROJECT_ROOT}/fdb.cluster"
  if [[ ! -f "${cluster_file}" ]]; then
    echo "ERROR: --fdb mode requires ${cluster_file}" >&2
    exit 1
  fi

  local fdb_lib="${FDB_LIBRARY_PATH_FDB_C:-/usr/local/lib/libfdb_c.dylib}"
  if [[ ! -f "${fdb_lib}" ]]; then
    echo "ERROR: --fdb mode requires FoundationDB client library at ${fdb_lib}" >&2
    echo "Set FDB_LIBRARY_PATH_FDB_C to the correct libfdb_c path." >&2
    exit 1
  fi

  if ! command -v fdbcli >/dev/null 2>&1; then
    echo "ERROR: --fdb mode requires 'fdbcli' to verify FoundationDB is running." >&2
    exit 1
  fi

  local status_output
  if ! status_output="$(fdbcli -C "${cluster_file}" --exec "status" 2>&1)"; then
    echo "ERROR: FoundationDB is not reachable using ${cluster_file}." >&2
    echo "Start FoundationDB first, then retry ./integration/run_integration.sh --fdb" >&2
    echo "fdbcli output:" >&2
    echo "${status_output}" >&2
    exit 1
  fi
}

run_and_expect() {
  local name="$1"
  local sql="$2"
  local expected="$3"

  echo "==> ${name}"
  local out
  if ! out="$(run_sql_raw "${sql}" 2>&1)"; then
    echo "SQL failed: ${sql}" >&2
    echo "Output:" >&2
    echo "${out}" >&2
    echo "--- SERVER LOG (tail) ---" >&2
    tail -n 80 "${SERVER_LOG}" >&2 || true
    echo "--- TRINO LOG (tail) ---" >&2
    tail -n 80 "${TRINO_LOG}" >&2 || true
    exit 1
  fi

  if ! grep -Eq "${expected}" <<<"${out}"; then
    echo "Assertion failed for: ${name}" >&2
    echo "Expected pattern: ${expected}" >&2
    echo "Actual output:" >&2
    echo "${out}" >&2
    exit 1
  fi
}

run_and_expect_fail() {
  local name="$1"
  local sql="$2"
  local expected="$3"

  echo "==> ${name}"
  local out
  if out="$(run_sql_raw "${sql}" 2>&1)"; then
    echo "Expected SQL to fail but it succeeded: ${sql}" >&2
    echo "Output:" >&2
    echo "${out}" >&2
    exit 1
  fi

  if ! grep -Eq "${expected}" <<<"${out}"; then
    echo "Assertion failed for expected failure: ${name}" >&2
    echo "Expected error pattern: ${expected}" >&2
    echo "Actual output:" >&2
    echo "${out}" >&2
    exit 1
  fi
}

run_http_expect() {
  local name="$1"
  local method="$2"
  local url="$3"
  local payload="${4:-}"
  local expected_status="$5"
  local expected_body_pattern="${6:-}"

  echo "==> ${name}"
  local response
  if [[ -n "${payload}" ]]; then
    response="$(curl -sS -X "${method}" -H "Content-Type: application/json" \
      -d "${payload}" -w $'\nHTTP_STATUS:%{http_code}' "${url}")"
  else
    response="$(curl -sS -X "${method}" -w $'\nHTTP_STATUS:%{http_code}' "${url}")"
  fi

  local status="${response##*HTTP_STATUS:}"
  local body="${response%$'\n'HTTP_STATUS:*}"

  if [[ "${status}" != "${expected_status}" ]]; then
    echo "HTTP assertion failed for: ${name}" >&2
    echo "Expected status: ${expected_status}, got: ${status}" >&2
    echo "Body:" >&2
    echo "${body}" >&2
    exit 1
  fi

  if [[ -n "${expected_body_pattern}" ]]; then
    if ! grep -Eq "${expected_body_pattern}" <<<"${body}"; then
      echo "HTTP body assertion failed for: ${name}" >&2
      echo "Expected pattern: ${expected_body_pattern}" >&2
      echo "Actual body:" >&2
      echo "${body}" >&2
      exit 1
    fi
  fi
}

extract_json_field() {
  local json="$1"
  local field="$2"
  echo "${json}" | sed -nE "s/.*\"${field}\":\"([^\"]+)\".*/\\1/p"
}

extract_last_path_segment() {
  local uri="$1"
  echo "${uri##*/}"
}

extract_metadata_version() {
  local filename="$1"
  echo "${filename}" | sed -nE 's/^([0-9]+)-.*\.metadata\.json$/\1/p'
}

require_port_free 8181 "Server"
require_port_free 8080 "Trino server"
if [[ "${SERVER_MODE}" == "fdb" ]]; then
  require_fdb_prereqs
fi

echo "Starting server..."
(
  cd "${PROJECT_ROOT}"
  if [[ ${#SERVER_GRADLE_ARGS[@]} -gt 0 ]]; then
    ./gradlew runIcebergRestServer "${SERVER_GRADLE_ARGS[@]}"
  else
    ./gradlew runIcebergRestServer
  fi
) >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

wait_for_http "http://localhost:8181/v1/config" "Server"

echo "Starting Trino server..."
"${TRINO_LAUNCHER}" -etc-dir "${TRINO_ETC}" run >"${TRINO_LOG}" 2>&1 &
TRINO_PID=$!

wait_for_trino

SCHEMA="it_${RUN_ID}"
TABLE="orders_${RUN_ID}"
RENAMED_TABLE="orders_renamed_${RUN_ID}"
POINTER_TABLE="pointer_${RUN_ID}"

run_and_expect "Create schema" \
  "CREATE SCHEMA IF NOT EXISTS iceberg.${SCHEMA}" \
  "CREATE SCHEMA"

run_and_expect "Create table" \
  "CREATE TABLE iceberg.${SCHEMA}.${TABLE} (order_id BIGINT, order_date DATE, amount DOUBLE)" \
  "CREATE TABLE"

run_and_expect_fail "Duplicate table create fails" \
  "CREATE TABLE iceberg.${SCHEMA}.${TABLE} (order_id BIGINT, order_date DATE, amount DOUBLE)" \
  "already exists|Table .* already exists"

ASSERT_CREATE_PAYLOAD=$(cat <<EOF
{"requirements":[{"type":"assert-create"}],"updates":[]}
EOF
)
run_http_expect "assert-create commit fails on existing table" \
  "POST" \
  "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${TABLE}" \
  "${ASSERT_CREATE_PAYLOAD}" \
  "409" \
  "assert-create failed"

echo "==> Metadata pointer advances across commits"
POINTER_CREATE_PAYLOAD=$(cat <<EOF
{"name":"${POINTER_TABLE}","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}}
EOF
)
run_http_expect "Create REST-only pointer test table" \
  "POST" \
  "http://localhost:8181/v1/namespaces/${SCHEMA}/tables" \
  "${POINTER_CREATE_PAYLOAD}" \
  "200"

GET_BEFORE="$(curl -sS -X GET "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${POINTER_TABLE}")"
LOC_BEFORE="$(extract_json_field "${GET_BEFORE}" "metadata-location")"
FILE_BEFORE="$(extract_last_path_segment "${LOC_BEFORE}")"
VER_BEFORE="$(extract_metadata_version "${FILE_BEFORE}")"
if [[ -z "${LOC_BEFORE}" ]]; then
  echo "Expected metadata-location before commit, got empty" >&2
  exit 1
fi
if [[ -z "${VER_BEFORE}" ]]; then
  echo "Unexpected metadata filename before commit: ${FILE_BEFORE}" >&2
  exit 1
fi

SNAP1=$((100000 + RANDOM))
COMMIT1=$(cat <<EOF
{"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":${SNAP1},"timestamp-ms":1773000000001,"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":${SNAP1},"type":"branch"}]}
EOF
)
run_http_expect "Commit #1 for metadata pointer test" \
  "POST" \
  "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${POINTER_TABLE}" \
  "${COMMIT1}" \
  "200"

GET_AFTER1="$(curl -sS -X GET "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${POINTER_TABLE}")"
LOC_AFTER1="$(extract_json_field "${GET_AFTER1}" "metadata-location")"
FILE_AFTER1="$(extract_last_path_segment "${LOC_AFTER1}")"
VER_AFTER1="$(extract_metadata_version "${FILE_AFTER1}")"
if [[ "${LOC_AFTER1}" == "${LOC_BEFORE}" ]]; then
  echo "metadata-location did not advance after first commit" >&2
  exit 1
fi
if [[ -z "${VER_AFTER1}" ]]; then
  echo "Unexpected metadata file after first commit: ${FILE_AFTER1}" >&2
  exit 1
fi
if (( 10#${VER_AFTER1} != 10#${VER_BEFORE} + 1 )); then
  echo "Metadata version did not increment by 1 after first commit: ${VER_BEFORE} -> ${VER_AFTER1}" >&2
  exit 1
fi

SNAP2=$((200000 + RANDOM))
COMMIT2=$(cat <<EOF
{"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":2,"snapshot-id":${SNAP2},"timestamp-ms":1773000000002,"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":${SNAP2},"type":"branch"}]}
EOF
)
run_http_expect "Commit #2 for metadata pointer test" \
  "POST" \
  "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${POINTER_TABLE}" \
  "${COMMIT2}" \
  "200"

GET_AFTER2="$(curl -sS -X GET "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${POINTER_TABLE}")"
LOC_AFTER2="$(extract_json_field "${GET_AFTER2}" "metadata-location")"
FILE_AFTER2="$(extract_last_path_segment "${LOC_AFTER2}")"
VER_AFTER2="$(extract_metadata_version "${FILE_AFTER2}")"
if [[ "${LOC_AFTER2}" == "${LOC_AFTER1}" ]]; then
  echo "metadata-location did not advance after second commit" >&2
  exit 1
fi
if [[ -z "${VER_AFTER2}" ]]; then
  echo "Unexpected metadata file after second commit: ${FILE_AFTER2}" >&2
  exit 1
fi
if (( 10#${VER_AFTER2} != 10#${VER_AFTER1} + 1 )); then
  echo "Metadata version did not increment by 1 after second commit: ${VER_AFTER1} -> ${VER_AFTER2}" >&2
  exit 1
fi

run_and_expect "Insert rows" \
  "INSERT INTO iceberg.${SCHEMA}.${TABLE} VALUES (1, DATE '2026-03-08', 100.50), (2, DATE '2026-03-09', 42.00)" \
  "INSERT:"

run_and_expect "Read rows" \
  "SELECT order_id, amount FROM iceberg.${SCHEMA}.${TABLE} ORDER BY order_id" \
  $'1\t100\\.5'

run_and_expect "Snapshots metadata" \
  "SELECT count(*) AS c FROM iceberg.${SCHEMA}.\"${TABLE}\$snapshots\"" \
  $'c\n[1-9][0-9]*'

run_and_expect "Schema evolution" \
  "ALTER TABLE iceberg.${SCHEMA}.${TABLE} ADD COLUMN note VARCHAR" \
  "ADD COLUMN"

run_and_expect "Insert with new column" \
  "INSERT INTO iceberg.${SCHEMA}.${TABLE} (order_id, order_date, amount, note) VALUES (3, DATE '2026-03-10', 77.25, 'new column test')" \
  "INSERT:"

run_and_expect "Read evolved schema" \
  "SELECT order_id, note FROM iceberg.${SCHEMA}.${TABLE} ORDER BY order_id" \
  "new column test"

run_and_expect "Rename table" \
  "ALTER TABLE iceberg.${SCHEMA}.${TABLE} RENAME TO iceberg.${SCHEMA}.${RENAMED_TABLE}" \
  "RENAME TABLE"

run_and_expect "Read renamed table" \
  "SELECT order_id, note FROM iceberg.${SCHEMA}.${RENAMED_TABLE} ORDER BY order_id" \
  "new column test"

run_and_expect_fail "Drop non-empty schema fails" \
  "DROP SCHEMA iceberg.${SCHEMA}" \
  "Schema not empty|Cannot drop non-empty schema|Namespace not empty"

run_and_expect "Drop renamed table" \
  "DROP TABLE iceberg.${SCHEMA}.${RENAMED_TABLE}" \
  "DROP TABLE"

run_http_expect "Drop REST-only pointer test table" \
  "DELETE" \
  "http://localhost:8181/v1/namespaces/${SCHEMA}/tables/${POINTER_TABLE}" \
  "" \
  "204"

run_and_expect "Drop empty schema" \
  "DROP SCHEMA iceberg.${SCHEMA}" \
  "DROP SCHEMA"

echo "Integration tests passed."
echo "Server log: ${SERVER_LOG}"
echo "Trino log: ${TRINO_LOG}"
