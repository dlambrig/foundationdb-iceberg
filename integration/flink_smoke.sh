#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/flink_server_${RUN_ID}.log"
FLINK_LOG="${LOG_DIR}/flink_smoke_${RUN_ID}.log"

START_SERVER=true
REPLACE_SERVER=false
SERVER_MODE="memory"
SERVER_GRADLE_ARGS=()

REST_URI="${REST_URI:-http://localhost:8181}"
WAREHOUSE_URI="${WAREHOUSE_URI:-file:///tmp/iceberg_warehouse}"
FLINK_IMAGE="${FLINK_IMAGE:-flink:2.1.0-scala_2.12-java17}"
ICEBERG_HOME="${ICEBERG_HOME:-/Users/dlambrig/iceberg}"
FLINK_ICEBERG_RUNTIME_JAR="${FLINK_ICEBERG_RUNTIME_JAR:-}"
HADOOP_CLIENT_API_JAR="${HADOOP_CLIENT_API_JAR:-/Users/dlambrig/spark-3.5.5/jars/hadoop-client-api-3.3.4.jar}"
HADOOP_CLIENT_RUNTIME_JAR="${HADOOP_CLIENT_RUNTIME_JAR:-/Users/dlambrig/spark-3.5.5/jars/hadoop-client-runtime-3.3.4.jar}"
COMMONS_LOGGING_JAR="${COMMONS_LOGGING_JAR:-/Users/dlambrig/spark-3.5.5/jars/commons-logging-1.1.3.jar}"

usage() {
  cat <<'EOF'
Usage: ./integration/flink_smoke.sh [--fdb] [--no-start-server] [--replace-server]

Runs a direct Flink SQL smoke flow against foundationdb-iceberg using a Dockerized
Flink SQL client plus a local Flink mini cluster inside the container.

Options:
  --fdb              Start foundationdb-iceberg in FDB mode (-Dfdb=true).
  --no-start-server  Reuse an existing server at REST_URI.
  --replace-server   Stop a conflicting local server on REST_URI's port and start a fresh one.
  -h, --help         Show help.

Environment overrides:
  REST_URI                   REST endpoint (default: http://localhost:8181)
  WAREHOUSE_URI              Warehouse URI shared with the Flink container
  FLINK_IMAGE                Docker image for Flink SQL client
  ICEBERG_HOME               Apache Iceberg repo path
  FLINK_ICEBERG_RUNTIME_JAR  Explicit iceberg-flink-runtime jar
  HADOOP_CLIENT_API_JAR      Hadoop client API jar path
  HADOOP_CLIENT_RUNTIME_JAR  Hadoop client runtime jar path
  COMMONS_LOGGING_JAR        commons-logging jar path
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
    --replace-server)
      REPLACE_SERVER=true
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
SQL_FILE=""
cleanup() {
  set +e
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SQL_FILE}" ]]; then
    rm -f "${SQL_FILE}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

rest_port() {
  local rest_no_scheme host_port
  rest_no_scheme="${REST_URI#*://}"
  host_port="${rest_no_scheme%%/*}"
  if [[ "${host_port}" == *:* ]]; then
    printf '%s\n' "${host_port##*:}"
  else
    printf '80\n'
  fi
}

list_port_pids() {
  local port="$1"
  lsof -ti tcp:"${port}" 2>/dev/null || true
}

server_is_healthy() {
  curl -sf "${REST_URI}/v1/config" >/dev/null 2>&1
}

stop_port_pids() {
  local port="$1"
  local pids
  pids="$(list_port_pids "${port}")"
  if [[ -z "${pids}" ]]; then
    return 0
  fi
  echo "Replacing existing server on port ${port}: ${pids}"
  kill ${pids} >/dev/null 2>&1 || true
  for _ in {1..20}; do
    if [[ -z "$(list_port_pids "${port}")" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Timed out waiting for existing process(es) on port ${port} to stop: ${pids}" >&2
  exit 1
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
    echo "ERROR: --fdb mode requires ${cluster_file}" >&2
    exit 1
  fi

  local fdb_lib="${FDB_LIBRARY_PATH_FDB_C:-/usr/local/lib/libfdb_c.dylib}"
  if [[ ! -f "${fdb_lib}" ]]; then
    echo "ERROR: --fdb mode requires FoundationDB client library at ${fdb_lib}" >&2
    echo "Set FDB_LIBRARY_PATH_FDB_C to the correct libfdb_c path." >&2
    exit 1
  fi
}

require_flink_prereqs() {
  if [[ -z "${FLINK_ICEBERG_RUNTIME_JAR}" ]]; then
    FLINK_ICEBERG_RUNTIME_JAR="$(
      find "${ICEBERG_HOME}/flink/v2.1/flink-runtime/build/libs" \
        -maxdepth 1 \
        -type f \
        -name 'iceberg-flink-runtime-2.1-*.jar' \
        ! -name '*-javadoc.jar' \
        ! -name '*-sources.jar' \
        ! -name '*-tests.jar' \
        | sort \
        | head -n1
    )"
  fi
  if [[ -z "${FLINK_ICEBERG_RUNTIME_JAR}" || ! -f "${FLINK_ICEBERG_RUNTIME_JAR}" ]]; then
    echo "ERROR: Flink smoke requires an iceberg-flink-runtime-2.1 jar." >&2
    echo "Expected under: ${ICEBERG_HOME}/flink/v2.1/flink-runtime/build/libs/" >&2
    echo "Set FLINK_ICEBERG_RUNTIME_JAR explicitly if needed." >&2
    exit 1
  fi

  local dependency
  for dependency in \
    "${HADOOP_CLIENT_API_JAR}" \
    "${HADOOP_CLIENT_RUNTIME_JAR}" \
    "${COMMONS_LOGGING_JAR}"; do
    if [[ ! -f "${dependency}" ]]; then
      echo "ERROR: Missing Flink dependency jar: ${dependency}" >&2
      exit 1
    fi
  done

  if ! docker image inspect "${FLINK_IMAGE}" >/dev/null 2>&1; then
    echo "ERROR: Docker image not available locally: ${FLINK_IMAGE}" >&2
    echo "Pull it first: docker pull ${FLINK_IMAGE}" >&2
    exit 1
  fi
}

assert_log_contains() {
  local pattern="$1"
  local message="$2"
  if ! grep -Eq "${pattern}" "${FLINK_LOG}"; then
    echo "ERROR: ${message}" >&2
    echo "See Flink log: ${FLINK_LOG}" >&2
    exit 1
  fi
}

REST_PORT="$(rest_port)"
if [[ "${SERVER_MODE}" == "fdb" ]]; then
  require_fdb_prereqs
fi
require_flink_prereqs

echo "==> Flink smoke setup"
echo "Server mode: ${SERVER_MODE}"
echo "REST URI: ${REST_URI}"
echo "Warehouse: ${WAREHOUSE_URI}"
echo "Flink image: ${FLINK_IMAGE}"
echo "Iceberg runtime jar: ${FLINK_ICEBERG_RUNTIME_JAR}"

if [[ "${START_SERVER}" == "true" ]]; then
  EXISTING_PIDS="$(list_port_pids "${REST_PORT}")"
  if [[ -n "${EXISTING_PIDS}" ]]; then
    if [[ "${REPLACE_SERVER}" == "true" ]]; then
      stop_port_pids "${REST_PORT}"
    elif server_is_healthy; then
      echo "Reusing existing server at ${REST_URI} (PID(s): ${EXISTING_PIDS})"
      START_SERVER=false
    else
      echo "ERROR: Server port ${REST_PORT} is already in use by PID(s): ${EXISTING_PIDS}" >&2
      echo "The existing listener is not responding as a healthy Iceberg REST server at ${REST_URI}." >&2
      echo "Use --replace-server to restart it, or stop the process(es) manually." >&2
      exit 1
    fi
  fi
fi

if [[ "${START_SERVER}" == "true" ]]; then
  (
    cd "${PROJECT_ROOT}"
    if [[ ${#SERVER_GRADLE_ARGS[@]} -gt 0 ]]; then
      ./gradlew runIcebergRestServer "${SERVER_GRADLE_ARGS[@]}"
    else
      ./gradlew runIcebergRestServer
    fi
  ) > "${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  wait_for_http "${REST_URI}/v1/config" "Server"
fi

NAMESPACE="flink_smoke_${RUN_ID}"
SQL_FILE="${LOG_DIR}/flink_smoke_${RUN_ID}.sql"
cat > "${SQL_FILE}" <<'SQL'
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
CREATE CATALOG rest WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://host.docker.internal:8181',
  'warehouse' = 'file:///tmp/iceberg_warehouse'
);
USE CATALOG rest;
CREATE DATABASE IF NOT EXISTS __NAMESPACE__;
USE __NAMESPACE__;
CREATE TABLE orders (id BIGINT, amount DOUBLE);
INSERT INTO orders VALUES (1, 10.5), (2, 20.25);
SELECT COUNT(*) AS c FROM orders;
DROP TABLE orders;
USE default_database;
DROP DATABASE __NAMESPACE__;
SQL
perl -0pi -e "s/__NAMESPACE__/${NAMESPACE}/g" "${SQL_FILE}"

echo "==> Running Flink smoke flow"
docker run --rm \
  --add-host host.docker.internal:host-gateway \
  -v "${SQL_FILE}":/work/flink_rest_smoke.sql:ro \
  -v "${FLINK_ICEBERG_RUNTIME_JAR}":/opt/flink/lib/iceberg-flink-runtime.jar:ro \
  -v "${HADOOP_CLIENT_API_JAR}":/opt/flink/lib/hadoop-client-api.jar:ro \
  -v "${HADOOP_CLIENT_RUNTIME_JAR}":/opt/flink/lib/hadoop-client-runtime.jar:ro \
  -v "${COMMONS_LOGGING_JAR}":/opt/flink/lib/commons-logging.jar:ro \
  -v /tmp/iceberg_warehouse:/tmp/iceberg_warehouse \
  "${FLINK_IMAGE}" \
  sh -lc '/opt/flink/bin/start-cluster.sh && /opt/flink/bin/sql-client.sh embedded -f /work/flink_rest_smoke.sql' \
  > "${FLINK_LOG}" 2>&1

assert_log_contains 'Complete execution of the SQL update statement' "Flink INSERT did not complete successfully"
assert_log_contains '\| 2 \|' "Flink query result did not show the expected row count"

echo
echo "Flink smoke passed."
echo "Server log: ${SERVER_LOG}"
echo "Flink log: ${FLINK_LOG}"
