#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/smoke_server_${RUN_ID}.log"
TRINO_LOG="${LOG_DIR}/smoke_trino_${RUN_ID}.log"
SERVER_MODE="memory"
SERVER_GRADLE_ARGS=()
START_SERVER=true
START_TRINO=true

usage() {
  cat <<'EOF'
Usage: ./integration/trino_smoke.sh [--fdb] [--no-start-server] [--no-start-trino]

Options:
  --fdb              Run IcebergRestServer in FoundationDB mode (-Dfdb=true).
  --no-start-server  Use an already-running server on localhost:8181.
  --no-start-trino   Use an already-running Trino server on localhost:8080.
  -h                 Show help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fdb)
      SERVER_MODE="fdb"
      SERVER_GRADLE_ARGS=(-Dfdb=true)
      shift
      ;;
    --no-start-server)
      START_SERVER=false
      shift
      ;;
    --no-start-trino)
      START_TRINO=false
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
TRINO_SERVER_URL="${TRINO_SERVER_URL:-http://localhost:8080}"
TRINO_ETC="${TRINO_ETC:-${TRINO_HOME}/etc}"

TRINO_CLI_JAR="${TRINO_CLI_JAR:-}"
if [[ -z "${TRINO_CLI_JAR}" ]]; then
  TRINO_CLI_JAR="$(ls "${TRINO_HOME}"/client/trino-cli/target/trino-cli-*-executable.jar 2>/dev/null | head -n1 || true)"
fi

if [[ -z "${TRINO_CLI_JAR}" || ! -f "${TRINO_CLI_JAR}" ]]; then
  echo "ERROR: Unable to find trino-cli executable jar. Set TRINO_CLI_JAR." >&2
  exit 1
fi

TRINO_SERVER_DIR="${TRINO_SERVER_DIR:-}"
if [[ -z "${TRINO_SERVER_DIR}" ]]; then
  TRINO_SERVER_DIR="$(ls -d "${TRINO_HOME}"/core/trino-server/target/trino-server-* 2>/dev/null | head -n1 || true)"
fi

TRINO_LAUNCHER="${TRINO_LAUNCHER:-}"
if [[ -z "${TRINO_LAUNCHER}" && -n "${TRINO_SERVER_DIR}" ]]; then
  if [[ -x "${TRINO_SERVER_DIR}/bin/darwin-amd64/launcher" ]]; then
    TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/darwin-amd64/launcher"
  elif [[ -x "${TRINO_SERVER_DIR}/bin/linux-amd64/launcher" ]]; then
    TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/linux-amd64/launcher"
  elif [[ -x "${TRINO_SERVER_DIR}/bin/launcher" ]]; then
    TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/launcher"
  fi
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

wait_for_http() {
  local url="$1"
  local name="$2"
  for _ in {1..90}; do
    if curl -sf "${url}" >/dev/null 2>&1; then
      echo "${name} is ready"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Timed out waiting for ${name}" >&2
  return 1
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
    echo "Start FoundationDB first, then retry ./integration/trino_smoke.sh --fdb" >&2
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
    [[ -f "${SERVER_LOG}" ]] && tail -n 80 "${SERVER_LOG}" >&2 || true
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

if [[ "${START_SERVER}" == "true" ]]; then
  require_port_free 8181 "Server"
fi
if [[ "${START_TRINO}" == "true" ]] && ! lsof -ti tcp:8080 >/dev/null 2>&1; then
  require_port_free 8080 "Trino"
fi

if [[ "${SERVER_MODE}" == "fdb" ]]; then
  require_fdb_prereqs
fi

echo "==> Trino smoke setup"
echo "Mode: ${SERVER_MODE}"
echo "Trino URL: ${TRINO_SERVER_URL}"
echo "CLI jar: ${TRINO_CLI_JAR}"

if [[ "${START_TRINO}" == "true" ]]; then
  if lsof -ti tcp:8080 >/dev/null 2>&1; then
    echo "Trino already running on :8080, using existing process"
  else
    if [[ -z "${TRINO_SERVER_DIR}" || ! -d "${TRINO_SERVER_DIR}" ]]; then
      echo "ERROR: Unable to find Trino server directory. Set TRINO_SERVER_DIR or use --no-start-trino." >&2
      exit 1
    fi
    if [[ -z "${TRINO_LAUNCHER}" || ! -x "${TRINO_LAUNCHER}" ]]; then
      echo "ERROR: Unable to find Trino launcher. Set TRINO_LAUNCHER or use --no-start-trino." >&2
      exit 1
    fi
    if [[ ! -d "${TRINO_ETC}" ]]; then
      echo "ERROR: TRINO_ETC does not exist: ${TRINO_ETC}" >&2
      exit 1
    fi
    echo "Starting Trino via ${TRINO_LAUNCHER}"
    nohup "${TRINO_LAUNCHER}" -etc-dir "${TRINO_ETC}" run >"${TRINO_LOG}" 2>&1 &
    TRINO_PID=$!
  fi
fi

if [[ "${START_SERVER}" == "true" ]]; then
  (
    cd "${PROJECT_ROOT}"
    if [[ "${SERVER_MODE}" == "fdb" ]]; then
      ./gradlew runIcebergRestServer "${SERVER_GRADLE_ARGS[@]}"
    else
      ./gradlew runIcebergRestServer
    fi
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
fi

wait_for_http "http://localhost:8181/v1/config" "Server"
wait_for_trino

SCHEMA="smoke_${RUN_ID}"
TABLE="orders"
VIEW="orders_v"

echo "==> Running Trino smoke checks"
run_and_expect "Create schema" "CREATE SCHEMA IF NOT EXISTS iceberg.${SCHEMA}" "^CREATE SCHEMA|already exists"
run_and_expect "Create table" "CREATE TABLE iceberg.${SCHEMA}.${TABLE} (order_id BIGINT, amount DOUBLE)" "^CREATE TABLE$"
run_and_expect "Insert rows" "INSERT INTO iceberg.${SCHEMA}.${TABLE} VALUES (1, 10.5), (2, 20.25)" "^INSERT: 2 rows$"
run_and_expect "Row count" "SELECT count(*) AS c FROM iceberg.${SCHEMA}.${TABLE}" "^[[:space:]]*2[[:space:]]*$"
run_and_expect "Describe table" "DESCRIBE iceberg.${SCHEMA}.${TABLE}" "order_id|amount"
run_and_expect "Create view" "CREATE VIEW iceberg.${SCHEMA}.${VIEW} AS SELECT order_id, amount FROM iceberg.${SCHEMA}.${TABLE}" "^CREATE VIEW$"
run_and_expect "View row count" "SELECT count(*) AS c FROM iceberg.${SCHEMA}.${VIEW}" "^[[:space:]]*2[[:space:]]*$"
run_and_expect "Show create view" "SHOW CREATE VIEW iceberg.${SCHEMA}.${VIEW}" "CREATE VIEW iceberg\\.${SCHEMA}\\.${VIEW}"
run_and_expect "Drop view" "DROP VIEW iceberg.${SCHEMA}.${VIEW}" "^DROP VIEW$"
run_and_expect "Show tables" "SHOW TABLES FROM iceberg.${SCHEMA}" "${TABLE}"
run_and_expect "Drop table" "DROP TABLE iceberg.${SCHEMA}.${TABLE}" "^DROP TABLE$"

echo "==> Cleaning schema"
run_and_expect "Drop schema" "DROP SCHEMA iceberg.${SCHEMA}" "^DROP SCHEMA$"

echo
echo "Trino smoke passed."
if [[ "${START_SERVER}" == "true" ]]; then
  echo "Server log: ${SERVER_LOG}"
fi
if [[ -n "${TRINO_PID}" ]]; then
  echo "Trino log: ${TRINO_LOG}"
fi
