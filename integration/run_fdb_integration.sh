#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/fdb_server_${RUN_ID}.log"
TRINO_LOG="${LOG_DIR}/fdb_trino_${RUN_ID}.log"
SERVER_GRADLE_ARGS=(-Dfdb=true)
CONCURRENT_ITERATIONS="${CONCURRENT_ITERATIONS:-20}"
MIXED_CONFLICT_ITERATIONS="${MIXED_CONFLICT_ITERATIONS:-12}"
WRITE_CYCLE_ITERATIONS="${WRITE_CYCLE_ITERATIONS:-18}"
SPARK_CONCURRENT_ITERATIONS="${SPARK_CONCURRENT_ITERATIONS:-2}"

START_SERVER=true
RUN_SMOKE=true
RUN_TRINO=true
RUN_SPARK=true
RUN_FLINK=true

SPARK_SQL_BIN="${SPARK_SQL_BIN:-spark-sql}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"
ICEBERG_HOME="${ICEBERG_HOME:-/Users/dlambrig/iceberg}"
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
CATALOG_NAME="${CATALOG_NAME:-rest}"
WAREHOUSE_URI="${WAREHOUSE_URI:-file:///tmp/fdb_iceberg_spark_warehouse}"
REST_URI="${REST_URI:-http://localhost:8181}"
SPARK_LOG="${LOG_DIR}/fdb_spark_${RUN_ID}.log"
SPARK_STATE_DIR="${LOG_DIR}/fdb_spark_state_${RUN_ID}"
ICEBERG_RUNTIME_JAR="${ICEBERG_RUNTIME_JAR:-}"
SPARK_JAVA_HOME="${SPARK_JAVA_HOME:-}"
TRINO_HOME="${TRINO_HOME:-/Users/dlambrig/trino}"
TRINO_SERVER_URL="${TRINO_SERVER_URL:-http://localhost:8080}"
TRINO_ETC="${TRINO_ETC:-${TRINO_HOME}/etc}"
TRINO_CLI_JAR="${TRINO_CLI_JAR:-}"
TRINO_SERVER_DIR="${TRINO_SERVER_DIR:-}"
TRINO_LAUNCHER="${TRINO_LAUNCHER:-}"
TRINO_RUNTIME_ETC=""
FLINK_LOG=""
FLINK_IMAGE="${FLINK_IMAGE:-flink:2.1.0-scala_2.12-java17}"
FLINK_ICEBERG_RUNTIME_JAR="${FLINK_ICEBERG_RUNTIME_JAR:-}"
HADOOP_CLIENT_API_JAR="${HADOOP_CLIENT_API_JAR:-/Users/dlambrig/spark-3.5.5/jars/hadoop-client-api-3.3.4.jar}"
HADOOP_CLIENT_RUNTIME_JAR="${HADOOP_CLIENT_RUNTIME_JAR:-/Users/dlambrig/spark-3.5.5/jars/hadoop-client-runtime-3.3.4.jar}"
COMMONS_LOGGING_JAR="${COMMONS_LOGGING_JAR:-/Users/dlambrig/spark-3.5.5/jars/commons-logging-1.1.3.jar}"

usage() {
  cat <<'EOF'
Usage: ./integration/run_fdb_integration.sh [--no-smoke] [--no-trino] [--no-spark] [--no-flink] [--no-start-server]

Options:
  --no-smoke         Skip ./integration/trino_smoke.sh --fdb pre-check.
  --no-trino         Skip direct Trino write/restart/read checks.
  --no-spark         Skip direct Spark write/restart/read checks.
  --no-flink         Skip direct Flink create/insert/query/drop checks.
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
    --no-spark)
      RUN_SPARK=false
      shift
      ;;
    --no-trino)
      RUN_TRINO=false
      shift
      ;;
    --no-flink)
      RUN_FLINK=false
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
mkdir -p "${SPARK_STATE_DIR}"

SERVER_PID=""
TRINO_PID=""

cleanup() {
  set +e
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TRINO_PID}" ]]; then
    kill "${TRINO_PID}" >/dev/null 2>&1 || true
    wait "${TRINO_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TRINO_RUNTIME_ETC}" ]]; then
    rm -rf "${TRINO_RUNTIME_ETC}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

list_port_pids() {
  local port="$1"
  lsof -ti tcp:"${port}" 2>/dev/null || true
}

require_port_free() {
  local port="$1"
  local name="$2"
  local pids
  pids="$(list_port_pids "${port}")"
  if [[ -n "${pids}" ]]; then
    echo "ERROR: ${name} port ${port} is already in use by PID(s): ${pids}" >&2
    echo "Stop existing process(es) first, or run: kill ${pids}" >&2
    exit 1
  fi
}

kill_port_processes() {
  local port="$1"
  local pids
  pids="$(list_port_pids "${port}")"
  if [[ -n "${pids}" ]]; then
    kill ${pids} >/dev/null 2>&1 || true
    sleep 1
    pids="$(list_port_pids "${port}")"
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

run_trino_sql_raw() {
  local sql="$1"
  java -jar "${TRINO_CLI_JAR}" --server "${TRINO_SERVER_URL}" --output-format TSV_HEADER --execute "${sql}"
}

wait_for_trino() {
  for _ in {1..120}; do
    if run_trino_sql_raw "SELECT 1" >/dev/null 2>&1; then
      echo "Trino is ready"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Timed out waiting for Trino" >&2
  return 1
}

prepare_trino_runtime_etc() {
  TRINO_RUNTIME_ETC="${LOG_DIR}/trino_etc_${RUN_ID}"
  rm -rf "${TRINO_RUNTIME_ETC}"
  mkdir -p "${TRINO_RUNTIME_ETC}"
  cp -R "${TRINO_ETC}/." "${TRINO_RUNTIME_ETC}/"

  local iceberg_catalog="${TRINO_RUNTIME_ETC}/catalog/iceberg.properties"
  if [[ ! -f "${iceberg_catalog}" ]]; then
    echo "ERROR: Missing Iceberg catalog config: ${iceberg_catalog}" >&2
    exit 1
  fi

  if rg -q '^local\.location=' "${iceberg_catalog}"; then
    perl -0pi -e 's/^local\.location=.*/local.location=\//m' "${iceberg_catalog}"
  else
    printf '\nlocal.location=/\n' >> "${iceberg_catalog}"
  fi
}

require_trino_prereqs() {
  if [[ -z "${TRINO_CLI_JAR}" ]]; then
    TRINO_CLI_JAR="$(ls "${TRINO_HOME}"/client/trino-cli/target/trino-cli-*-executable.jar 2>/dev/null | head -n1 || true)"
  fi
  if [[ -z "${TRINO_CLI_JAR}" || ! -f "${TRINO_CLI_JAR}" ]]; then
    echo "ERROR: Unable to find trino-cli executable jar. Set TRINO_CLI_JAR." >&2
    exit 1
  fi

  if [[ -z "${TRINO_SERVER_DIR}" ]]; then
    TRINO_SERVER_DIR="$(ls -d "${TRINO_HOME}"/core/trino-server/target/trino-server-* 2>/dev/null | head -n1 || true)"
  fi
  if [[ -z "${TRINO_SERVER_DIR}" || ! -d "${TRINO_SERVER_DIR}" ]]; then
    echo "ERROR: Unable to find Trino server directory. Set TRINO_SERVER_DIR." >&2
    exit 1
  fi

  if [[ -z "${TRINO_LAUNCHER}" ]]; then
    if [[ -x "${TRINO_SERVER_DIR}/bin/darwin-amd64/launcher" ]]; then
      TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/darwin-amd64/launcher"
    elif [[ -x "${TRINO_SERVER_DIR}/bin/linux-amd64/launcher" ]]; then
      TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/linux-amd64/launcher"
    elif [[ -x "${TRINO_SERVER_DIR}/bin/launcher" ]]; then
      TRINO_LAUNCHER="${TRINO_SERVER_DIR}/bin/launcher"
    fi
  fi
  if [[ -z "${TRINO_LAUNCHER}" || ! -x "${TRINO_LAUNCHER}" ]]; then
    echo "ERROR: Unable to find Trino launcher. Set TRINO_LAUNCHER." >&2
    exit 1
  fi

  if [[ ! -d "${TRINO_ETC}" ]]; then
    echo "ERROR: TRINO_ETC does not exist: ${TRINO_ETC}" >&2
    exit 1
  fi
}

spark_version_output() {
  "${SPARK_SQL_BIN}" --version 2>&1
}

find_compatible_spark_sql() {
  local candidate candidates=()

  if [[ -n "${SPARK_SQL_BIN:-}" ]]; then
    candidates+=("${SPARK_SQL_BIN}")
  fi

  candidates+=(
    "${HOME}/spark-3.5.5/bin/spark-sql"
    "${HOME}/spark-3.5.4/bin/spark-sql"
    "${HOME}/spark-3.5.3/bin/spark-sql"
    "${HOME}/spark-3.5.2/bin/spark-sql"
    "${HOME}/spark-3.5.1/bin/spark-sql"
    "${HOME}/spark-3.5.0/bin/spark-sql"
  )

  if command -v spark-sql >/dev/null 2>&1; then
    candidates+=("$(command -v spark-sql)")
  fi

  for candidate in "${candidates[@]}"; do
    if [[ -z "${candidate}" ]]; then
      continue
    fi
    if [[ ! -x "${candidate}" ]]; then
      continue
    fi
    local version_output spark_version_line scala_version_line
    version_output="$("${candidate}" --version 2>&1 || true)"
    spark_version_line="$(printf '%s\n' "${version_output}" | rg -m1 'version [0-9]+\.[0-9]+\.[0-9]+' || true)"
    scala_version_line="$(printf '%s\n' "${version_output}" | rg -m1 'Scala version [0-9]+\.[0-9]+' || true)"
    if [[ "${spark_version_line}" == *"version ${SPARK_VERSION}."* ]] && [[ "${scala_version_line}" == *"Scala version ${SCALA_VERSION}."* ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

validate_spark_binary() {
  local version_output spark_version_line scala_version_line
  version_output="$(spark_version_output)"
  spark_version_line="$(printf '%s\n' "${version_output}" | rg -m1 'version [0-9]+\.[0-9]+\.[0-9]+' || true)"
  scala_version_line="$(printf '%s\n' "${version_output}" | rg -m1 'Scala version [0-9]+\.[0-9]+' || true)"

  if [[ "${spark_version_line}" != *"version ${SPARK_VERSION}."* ]]; then
    echo "ERROR: spark-sql version does not match SPARK_VERSION=${SPARK_VERSION}" >&2
    echo "Detected: ${spark_version_line:-unknown}" >&2
    echo "Set SPARK_SQL_BIN to a Spark ${SPARK_VERSION}.x binary." >&2
    return 1
  fi

  if [[ "${scala_version_line}" != *"Scala version ${SCALA_VERSION}."* ]]; then
    echo "ERROR: spark-sql Scala version does not match SCALA_VERSION=${SCALA_VERSION}" >&2
    echo "Detected: ${scala_version_line:-unknown}" >&2
    echo "Set SPARK_SQL_BIN to a Spark build using Scala ${SCALA_VERSION}.x." >&2
    return 1
  fi
}

require_spark_prereqs() {
  if ! command -v "${SPARK_SQL_BIN}" >/dev/null 2>&1; then
    local compatible_spark_sql
    compatible_spark_sql="$(find_compatible_spark_sql || true)"
    if [[ -n "${compatible_spark_sql}" ]]; then
      SPARK_SQL_BIN="${compatible_spark_sql}"
    else
      echo "ERROR: Spark checks require spark-sql (${SPARK_SQL_BIN})" >&2
      exit 1
    fi
  fi

  if ! validate_spark_binary >/dev/null 2>&1; then
    local compatible_spark_sql
    compatible_spark_sql="$(find_compatible_spark_sql || true)"
    if [[ -n "${compatible_spark_sql}" && "${compatible_spark_sql}" != "${SPARK_SQL_BIN}" ]]; then
      echo "Using compatible Spark binary: ${compatible_spark_sql}"
      SPARK_SQL_BIN="${compatible_spark_sql}"
    fi
  fi

  validate_spark_binary

  if [[ -z "${ICEBERG_RUNTIME_JAR}" ]]; then
    ICEBERG_RUNTIME_JAR="$(ls "${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark-runtime/build/libs"/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-*.jar 2>/dev/null | head -n1 || true)"
  fi
  if [[ -z "${ICEBERG_RUNTIME_JAR}" || ! -f "${ICEBERG_RUNTIME_JAR}" ]]; then
    echo "ERROR: Spark checks require Iceberg Spark runtime jar under ${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark-runtime/build/libs/" >&2
    echo "Set ICEBERG_RUNTIME_JAR explicitly or build the runtime jar first." >&2
    exit 1
  fi

  if [[ -z "${SPARK_JAVA_HOME}" ]]; then
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
      SPARK_JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
    fi
  fi
  if [[ -z "${SPARK_JAVA_HOME}" || ! -d "${SPARK_JAVA_HOME}" ]]; then
    echo "ERROR: Spark checks require JDK 21. Set SPARK_JAVA_HOME to a JDK 21 installation." >&2
    exit 1
  fi
}

run_spark_sql_file() {
  local sql_file="$1"
  local log_file="${2:-${SPARK_LOG}}"
  env JAVA_HOME="${SPARK_JAVA_HOME}" PATH="${SPARK_JAVA_HOME}/bin:${PATH}" "${SPARK_SQL_BIN}" \
    --master "${SPARK_MASTER}" \
    --jars "${ICEBERG_RUNTIME_JAR}" \
    --driver-java-options "-Dderby.system.home=${SPARK_STATE_DIR}" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.${CATALOG_NAME}.type=rest" \
    --conf "spark.sql.catalog.${CATALOG_NAME}.uri=${REST_URI}" \
    --conf "spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_URI}" \
    --conf "spark.sql.warehouse.dir=${SPARK_STATE_DIR}/spark-warehouse" \
    --conf "spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.hadoop.HadoopFileIO" \
    -f "${sql_file}" >> "${log_file}" 2>&1
}

assert_spark_log_contains() {
  local pattern="$1"
  local message="$2"
  if ! grep -Eq "${pattern}" "${SPARK_LOG}"; then
    echo "ERROR: ${message}" >&2
    echo "See Spark log: ${SPARK_LOG}" >&2
    exit 1
  fi
}

run_spark_restart_checks() {
  local spark_ns="spark_fdb_${RUN_ID}"
  local spark_table="orders"
  local spark_view="orders_v"
  local bootstrap_sql="${LOG_DIR}/fdb_spark_bootstrap_${RUN_ID}.sql"
  local reload_sql="${LOG_DIR}/fdb_spark_reload_${RUN_ID}.sql"

  echo "==> Spark bootstrap before restart"
  cat > "${bootstrap_sql}" <<EOF
CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${spark_ns};
CREATE TABLE ${CATALOG_NAME}.${spark_ns}.${spark_table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${spark_ns}.${spark_table} VALUES (1, 10.5), (2, 20.25);
CREATE VIEW ${CATALOG_NAME}.${spark_ns}.${spark_view} AS SELECT order_id, amount FROM ${CATALOG_NAME}.${spark_ns}.${spark_table};
SELECT COUNT(*) AS spark_pre_restart_count FROM ${CATALOG_NAME}.${spark_ns}.${spark_table};
SELECT COUNT(*) AS spark_pre_restart_view_count FROM ${CATALOG_NAME}.${spark_ns}.${spark_view};
EOF
  : > "${SPARK_LOG}"
  run_spark_sql_file "${bootstrap_sql}"
  assert_spark_log_contains '(^|[[:space:]])2($|[[:space:]])' "Spark bootstrap did not produce expected row counts before restart."

  echo "Stopping server for Spark restart checks..."
  stop_server
  require_port_free 8181 "Server"

  echo "Restarting server for Spark reload checks..."
  start_server

  echo "==> Spark reload after restart"
  cat > "${reload_sql}" <<EOF
SELECT COUNT(*) AS spark_post_restart_count FROM ${CATALOG_NAME}.${spark_ns}.${spark_table};
SELECT COUNT(*) AS spark_post_restart_view_count FROM ${CATALOG_NAME}.${spark_ns}.${spark_view};
SELECT COUNT(*) AS spark_post_restart_snapshot_count FROM ${CATALOG_NAME}.${spark_ns}.${spark_table}.snapshots;
DROP VIEW ${CATALOG_NAME}.${spark_ns}.${spark_view};
DROP TABLE ${CATALOG_NAME}.${spark_ns}.${spark_table};
DROP NAMESPACE ${CATALOG_NAME}.${spark_ns};
EOF
  run_spark_sql_file "${reload_sql}"
  assert_spark_log_contains '(^|[[:space:]])2($|[[:space:]])' "Spark reload did not preserve expected table/view row counts after restart."
  assert_spark_log_contains '(^|[[:space:]])1($|[[:space:]])' "Spark reload did not show a snapshot count after restart."
}

run_spark_concurrent_writer_checks() {
  local spark_ns="spark_concurrent_${RUN_ID}"
  local spark_table="orders"
  local bootstrap_sql="${LOG_DIR}/fdb_spark_concurrent_bootstrap_${RUN_ID}.sql"
  local verify_sql="${LOG_DIR}/fdb_spark_concurrent_verify_${RUN_ID}.sql"
  local writer1_sql="${LOG_DIR}/fdb_spark_concurrent_writer1_${RUN_ID}.sql"
  local writer2_sql="${LOG_DIR}/fdb_spark_concurrent_writer2_${RUN_ID}.sql"
  local writer1_log="${LOG_DIR}/fdb_spark_concurrent_writer1_${RUN_ID}.log"
  local writer2_log="${LOG_DIR}/fdb_spark_concurrent_writer2_${RUN_ID}.log"

  echo "==> Concurrent Spark writer checks"
  cat > "${bootstrap_sql}" <<EOF
CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${spark_ns};
DROP TABLE IF EXISTS ${CATALOG_NAME}.${spark_ns}.${spark_table};
CREATE TABLE ${CATALOG_NAME}.${spark_ns}.${spark_table} (order_id BIGINT, amount DOUBLE) USING iceberg;
EOF
  : > "${SPARK_LOG}"
  run_spark_sql_file "${bootstrap_sql}"

  for ((i = 1; i <= SPARK_CONCURRENT_ITERATIONS; i++)); do
    local base1 base2
    base1=$(( (i - 1) * 200 + 1 ))
    base2=$(( base1 + 100 ))

    cat > "${writer1_sql}" <<EOF
INSERT INTO ${CATALOG_NAME}.${spark_ns}.${spark_table}
SELECT CAST(id AS BIGINT) AS order_id, CAST(id AS DOUBLE) AS amount
FROM (
  SELECT explode(sequence(${base1}, $((base1 + 99)))) AS id
) s;
EOF
    cat > "${writer2_sql}" <<EOF
INSERT INTO ${CATALOG_NAME}.${spark_ns}.${spark_table}
SELECT CAST(id AS BIGINT) AS order_id, CAST(id AS DOUBLE) AS amount
FROM (
  SELECT explode(sequence(${base2}, $((base2 + 99)))) AS id
) s;
EOF

    : > "${writer1_log}"
    : > "${writer2_log}"
    run_spark_sql_file "${writer1_sql}" "${writer1_log}" &
    local pid1=$!
    run_spark_sql_file "${writer2_sql}" "${writer2_log}" &
    local pid2=$!

    local status1=0
    local status2=0
    wait "${pid1}" || status1=$?
    wait "${pid2}" || status2=$?

    if [[ "${status1}" -ne 0 || "${status2}" -ne 0 ]]; then
      echo "ERROR: Concurrent Spark writer iteration ${i} failed (statuses: ${status1}, ${status2})." >&2
      echo "Writer logs:" >&2
      echo "  ${writer1_log}" >&2
      echo "  ${writer2_log}" >&2
      exit 1
    fi
  done

  cat > "${verify_sql}" <<EOF
SELECT COUNT(*) AS total_rows FROM ${CATALOG_NAME}.${spark_ns}.${spark_table};
SELECT COUNT(DISTINCT order_id) AS distinct_rows FROM ${CATALOG_NAME}.${spark_ns}.${spark_table};
DROP TABLE ${CATALOG_NAME}.${spark_ns}.${spark_table};
DROP NAMESPACE ${CATALOG_NAME}.${spark_ns};
EOF
  : > "${SPARK_LOG}"
  run_spark_sql_file "${verify_sql}"
  local expected_rows=$((SPARK_CONCURRENT_ITERATIONS * 200))
  assert_spark_log_contains "(^|[[:space:]])${expected_rows}($|[[:space:]])" "Concurrent Spark writers did not preserve the expected total row count."
}

run_flink_smoke_checks() {
  echo "==> Flink smoke checks"
  local before_log=""
  before_log="$(ls -1t "${LOG_DIR}"/flink_smoke_*.log 2>/dev/null | head -n1 || true)"

  (
    cd "${PROJECT_ROOT}"
    ./integration/flink_smoke.sh --fdb --no-start-server --scenario basic,schema_evolution
  )

  FLINK_LOG="$(ls -1t "${LOG_DIR}"/flink_smoke_*.log 2>/dev/null | head -n1 || true)"
  if [[ -z "${FLINK_LOG}" || "${FLINK_LOG}" == "${before_log}" ]]; then
    echo "ERROR: Could not determine Flink smoke log for this run." >&2
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
    echo "ERROR: Flink checks require an iceberg-flink-runtime-2.1 jar." >&2
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

run_flink_sql_file() {
  local sql_file="$1"
  docker run --rm \
    --add-host host.docker.internal:host-gateway \
    -v "${sql_file}":/work/flink_rest_flow.sql:ro \
    -v "${FLINK_ICEBERG_RUNTIME_JAR}":/opt/flink/lib/iceberg-flink-runtime.jar:ro \
    -v "${HADOOP_CLIENT_API_JAR}":/opt/flink/lib/hadoop-client-api.jar:ro \
    -v "${HADOOP_CLIENT_RUNTIME_JAR}":/opt/flink/lib/hadoop-client-runtime.jar:ro \
    -v "${COMMONS_LOGGING_JAR}":/opt/flink/lib/commons-logging.jar:ro \
    -v /tmp/iceberg_warehouse:/tmp/iceberg_warehouse \
    "${FLINK_IMAGE}" \
    sh -lc '/opt/flink/bin/start-cluster.sh && /opt/flink/bin/sql-client.sh embedded -f /work/flink_rest_flow.sql'
}

assert_flink_log_contains() {
  local pattern="$1"
  local message="$2"
  if ! grep -Eq "${pattern}" "${FLINK_LOG}"; then
    echo "ERROR: ${message}" >&2
    echo "See Flink log: ${FLINK_LOG}" >&2
    exit 1
  fi
}

run_flink_restart_checks() {
  local flink_ns="flink_fdb_${RUN_ID}"
  local flink_table="orders"
  local bootstrap_sql="${LOG_DIR}/fdb_flink_bootstrap_${RUN_ID}.sql"
  local reload_sql="${LOG_DIR}/fdb_flink_reload_${RUN_ID}.sql"

  echo "==> Flink bootstrap before restart"
  cat > "${bootstrap_sql}" <<EOF
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
CREATE DATABASE IF NOT EXISTS ${flink_ns};
USE ${flink_ns};
CREATE TABLE ${flink_table} (id BIGINT, amount DOUBLE);
INSERT INTO ${flink_table} VALUES (1, 10.5), (2, 20.25);
ALTER TABLE ${flink_table} ADD note STRING;
INSERT INTO ${flink_table} VALUES (3, 30.75, 'evolved');
SELECT COUNT(*) AS flink_pre_restart_count FROM ${flink_table};
SELECT COUNT(note) AS flink_pre_restart_noted FROM ${flink_table};
EOF
  run_flink_sql_file "${bootstrap_sql}" >> "${FLINK_LOG}" 2>&1
  assert_flink_log_contains '[|][[:space:]]*3[[:space:]]*[|]' "Flink bootstrap did not preserve the expected total row count before restart."
  assert_flink_log_contains '[|][[:space:]]*1[[:space:]]*[|]' "Flink bootstrap did not preserve the expected evolved-column row count before restart."

  echo "Stopping server for Flink restart checks..."
  stop_server
  require_port_free 8181 "Server"

  echo "Restarting server for Flink reload checks..."
  start_server

  echo "==> Flink reload after restart"
  cat > "${reload_sql}" <<EOF
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
USE ${flink_ns};
SELECT COUNT(*) AS flink_post_restart_count FROM ${flink_table};
SELECT COUNT(note) AS flink_post_restart_noted FROM ${flink_table};
DESCRIBE ${flink_table};
DROP TABLE ${flink_table};
USE CATALOG default_catalog;
DROP DATABASE rest.${flink_ns};
EOF
  run_flink_sql_file "${reload_sql}" >> "${FLINK_LOG}" 2>&1
  assert_flink_log_contains '[|][[:space:]]*3[[:space:]]*[|]' "Flink reload did not preserve the expected total row count after restart."
  assert_flink_log_contains '[|][[:space:]]*1[[:space:]]*[|]' "Flink reload did not preserve the expected evolved-column row count after restart."
  assert_flink_log_contains 'note[[:space:]]+[|][[:space:]]+STRING' "Flink reload did not show the evolved column after restart."
}

run_trino_and_expect() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  echo "==> ${name}"
  local out
  if ! out="$(run_trino_sql_raw "${sql}" 2>&1)"; then
    echo "Trino SQL failed: ${sql}" >&2
    echo "${out}" >&2
    echo "See Trino log: ${TRINO_LOG}" >&2
    exit 1
  fi
  if ! grep -Eq "${expected}" <<<"${out}"; then
    echo "Trino assertion failed for ${name}" >&2
    echo "Expected pattern: ${expected}" >&2
    echo "Actual output:" >&2
    echo "${out}" >&2
    exit 1
  fi
}

run_trino_restart_checks() {
  local trino_schema="trino_fdb_${RUN_ID}"
  local trino_table="orders"
  local trino_view="orders_v"

  echo "==> Trino bootstrap before restart"
  run_trino_and_expect "Trino create schema" "CREATE SCHEMA IF NOT EXISTS iceberg.${trino_schema}" "^CREATE SCHEMA|already exists"
  run_trino_and_expect "Trino create table" "CREATE TABLE iceberg.${trino_schema}.${trino_table} (order_id BIGINT, amount DOUBLE)" "^CREATE TABLE$"
  run_trino_and_expect "Trino insert rows" "INSERT INTO iceberg.${trino_schema}.${trino_table} VALUES (1, 10.5), (2, 20.25)" "^INSERT: 2 rows$"
  run_trino_and_expect "Trino create view" "CREATE VIEW iceberg.${trino_schema}.${trino_view} AS SELECT order_id, amount FROM iceberg.${trino_schema}.${trino_table}" "^CREATE VIEW$"
  run_trino_and_expect "Trino pre-restart count" "SELECT count(*) AS c FROM iceberg.${trino_schema}.${trino_table}" "^[[:space:]]*2[[:space:]]*$"
  run_trino_and_expect "Trino pre-restart view count" "SELECT count(*) AS c FROM iceberg.${trino_schema}.${trino_view}" "^[[:space:]]*2[[:space:]]*$"

  echo "Stopping server for Trino restart checks..."
  stop_server
  require_port_free 8181 "Server"

  echo "Restarting server for Trino reload checks..."
  start_server
  ensure_trino_running

  echo "==> Trino reload after restart"
  run_trino_and_expect "Trino post-restart table count" "SELECT count(*) AS c FROM iceberg.${trino_schema}.${trino_table}" "^[[:space:]]*2[[:space:]]*$"
  run_trino_and_expect "Trino post-restart view count" "SELECT count(*) AS c FROM iceberg.${trino_schema}.${trino_view}" "^[[:space:]]*2[[:space:]]*$"
  run_trino_and_expect "Trino post-restart snapshot count" "SELECT count(*) AS c FROM iceberg.${trino_schema}.\"${trino_table}\$snapshots\"" "^[[:space:]]*[1-9][0-9]*[[:space:]]*$"
  run_trino_and_expect "Trino drop view" "DROP VIEW iceberg.${trino_schema}.${trino_view}" "^DROP VIEW$"
  run_trino_and_expect "Trino drop table" "DROP TABLE iceberg.${trino_schema}.${trino_table}" "^DROP TABLE$"
  run_trino_and_expect "Trino drop schema" "DROP SCHEMA iceberg.${trino_schema}" "^DROP SCHEMA$"
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

assert_concurrent_outcomes() {
  local name="$1"
  local status1="$2"
  local status2="$3"
  if ! { [[ "${status1}" == "200" && "${status2}" == "409" ]] || [[ "${status1}" == "409" && "${status2}" == "200" ]]; }; then
    echo "${name} failed: expected one 200 and one 409, got ${status1} and ${status2}" >&2
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

start_trino() {
  kill_port_processes 8080
  require_port_free 8080 "Trino"
  prepare_trino_runtime_etc
  nohup "${TRINO_LAUNCHER}" -etc-dir "${TRINO_RUNTIME_ETC}" run >"${TRINO_LOG}" 2>&1 &
  TRINO_PID=$!
  wait_for_trino
}

stop_trino() {
  if [[ -n "${TRINO_PID}" ]]; then
    kill "${TRINO_PID}" >/dev/null 2>&1 || true
    wait "${TRINO_PID}" >/dev/null 2>&1 || true
    TRINO_PID=""
  fi
  kill_port_processes 8080
}

ensure_trino_running() {
  if run_trino_sql_raw "SELECT 1" >/dev/null 2>&1; then
    return 0
  fi
  echo "Trino is not responding after server restart; starting a fresh Trino instance..."
  start_trino
}

require_fdb_prereqs
if [[ "${RUN_TRINO}" == "true" || "${RUN_SMOKE}" == "true" ]]; then
  require_trino_prereqs
fi
if [[ "${RUN_SPARK}" == "true" ]]; then
  require_spark_prereqs
fi
if [[ "${RUN_FLINK}" == "true" ]]; then
  require_flink_prereqs
  if [[ ! -x "${PROJECT_ROOT}/integration/flink_smoke.sh" ]]; then
    echo "ERROR: Flink integration requires executable ${PROJECT_ROOT}/integration/flink_smoke.sh" >&2
    exit 1
  fi
fi

if [[ "${RUN_SMOKE}" == "true" ]]; then
  echo "==> Running Trino smoke in FDB mode"
  "${PROJECT_ROOT}/integration/trino_smoke.sh" --fdb --replace-server --replace-trino
fi

echo "==> Starting FDB-backed server for REST checks"
start_server
if [[ "${RUN_TRINO}" == "true" ]]; then
  echo "==> Starting Trino for FDB-backed checks"
  start_trino
fi

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

  if [[ "${RUN_SPARK}" == "true" ]]; then
    run_spark_restart_checks
    run_spark_concurrent_writer_checks
  fi
  if [[ "${RUN_TRINO}" == "true" ]]; then
    run_trino_restart_checks
  fi
  if [[ "${RUN_FLINK}" == "true" ]]; then
    run_flink_smoke_checks
    run_flink_restart_checks
  fi
else
  echo "Skipping restart/reload checks because --no-start-server was set."
fi

echo "==> Repeated concurrent writer conflict checks"
for ((i = 1; i <= CONCURRENT_ITERATIONS; i++)); do
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

  assert_concurrent_outcomes "Concurrent writer check iteration ${i}" "${STATUS1}" "${STATUS2}"
done

echo "==> Mixed update-action conflict checks"
for ((i = 1; i <= MIXED_CONFLICT_ITERATIONS; i++)); do
  MX_TABLE="mx_${RUN_ID}_${i}"
  MX_CREATE_PAYLOAD="{\"name\":\"${MX_TABLE}\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}"
  MX_CREATE_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables" "${MX_CREATE_PAYLOAD}")"
  assert_http_status "create mixed-conflict table ${i}" "${MX_CREATE_RESP}" "200"

  SNAP_A=$((810000 + i))
  SNAP_B=$((910000 + i))
  PAYLOAD_A='{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":'"${SNAP_A}"',"timestamp-ms":1773011000'"${i}"',"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":'"${SNAP_A}"',"type":"branch"},{"action":"set-properties","updates":{"writer":"A","batch":"'"${i}"'"}}]}'
  PAYLOAD_B='{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":'"${SNAP_B}"',"timestamp-ms":1773012000'"${i}"',"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":'"${SNAP_B}"',"type":"branch"},{"action":"add-schema","schema":{"type":"struct","schema-id":2,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"note","required":false,"type":"string"}]},"last-column-id":2},{"action":"set-current-schema","schema-id":2}]}'

  MA_FILE="$(mktemp)"
  MB_FILE="$(mktemp)"
  (
    run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${MX_TABLE}" "${PAYLOAD_A}" >"${MA_FILE}"
  ) &
  PID_A=$!
  (
    run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${MX_TABLE}" "${PAYLOAD_B}" >"${MB_FILE}"
  ) &
  PID_B=$!

  wait "${PID_A}"
  wait "${PID_B}"

  STATUS_A="$(http_status "$(cat "${MA_FILE}")")"
  STATUS_B="$(http_status "$(cat "${MB_FILE}")")"
  rm -f "${MA_FILE}" "${MB_FILE}"

  assert_concurrent_outcomes "Mixed conflict check iteration ${i}" "${STATUS_A}" "${STATUS_B}"
done

if [[ "${START_SERVER}" == "true" ]]; then
  echo "==> Restart during write cycle checks"
  RW_TABLE="rw_${RUN_ID}"
  RW_CREATE_PAYLOAD="{\"name\":\"${RW_TABLE}\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}"
  RW_CREATE_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables" "${RW_CREATE_PAYLOAD}")"
  assert_http_status "create restart-write table" "${RW_CREATE_RESP}" "200"

  CURRENT_SNAPSHOT="null"
  HALF_POINT=$((WRITE_CYCLE_ITERATIONS / 2))
  for ((i = 1; i <= WRITE_CYCLE_ITERATIONS; i++)); do
    SNAP=$((990000 + i))
    WRITE_COMMIT='{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":'"${CURRENT_SNAPSHOT}"'}],"updates":[{"action":"add-snapshot","snapshot":{"sequence-number":'"${i}"',"snapshot-id":'"${SNAP}"',"timestamp-ms":1773013000'"${i}"',"schema-id":0}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":'"${SNAP}"',"type":"branch"},{"action":"set-properties","updates":{"write-cycle":"'"${i}"'"}}]}'
    WRITE_RESP="$(run_http_request "POST" "http://localhost:8181/v1/namespaces/${NS}/tables/${RW_TABLE}" "${WRITE_COMMIT}")"
    assert_http_status "write-cycle commit ${i}" "${WRITE_RESP}" "200"
    CURRENT_SNAPSHOT="${SNAP}"

    if [[ "${i}" -eq "${HALF_POINT}" ]]; then
      stop_server
      start_server
    fi
  done

  RW_GET="$(run_http_request "GET" "http://localhost:8181/v1/namespaces/${NS}/tables/${RW_TABLE}")"
  assert_http_status "get table after write-cycle restarts" "${RW_GET}" "200"
  assert_body_contains "write-cycle current snapshot" "${RW_GET}" "\"current-snapshot-id\":${CURRENT_SNAPSHOT}"
else
  echo "Skipping restart-during-write checks because --no-start-server was set."
fi

echo
echo "FDB integration checks passed."
if [[ -n "${SERVER_LOG}" ]]; then
  echo "Server log: ${SERVER_LOG}"
fi
if [[ "${RUN_SPARK}" == "true" ]]; then
  echo "Spark log: ${SPARK_LOG}"
fi
if [[ "${RUN_TRINO}" == "true" ]]; then
  echo "Trino log: ${TRINO_LOG}"
fi
if [[ "${RUN_FLINK}" == "true" && -n "${FLINK_LOG}" ]]; then
  echo "Flink log: ${FLINK_LOG}"
fi
