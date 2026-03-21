#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/spark_server_${RUN_ID}.log"
SPARK_LOG="${LOG_DIR}/spark_smoke_${RUN_ID}.log"
SPARK_STATE_DIR="${LOG_DIR}/spark_state_${RUN_ID}"

START_SERVER=true
REPLACE_SERVER=false
SERVER_MODE="memory"
SERVER_GRADLE_ARGS=()

SPARK_SQL_BIN="${SPARK_SQL_BIN:-spark-sql}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"
ICEBERG_HOME="${ICEBERG_HOME:-/Users/dlambrig/iceberg}"
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
CATALOG_NAME="${CATALOG_NAME:-rest}"
WAREHOUSE_URI="${WAREHOUSE_URI:-file:///tmp/fdb_iceberg_spark_warehouse}"
REST_URI="${REST_URI:-http://localhost:8181}"
SCENARIOS="${SCENARIOS:-basic,schema_evolution,overwrite,snapshots,replace_table,views}"

usage() {
  cat <<'EOF'
Usage: ./integration/spark_smoke.sh [--fdb] [--no-start-server] [--replace-server] [--scenario name[,name...] ]

Runs direct Spark SQL compatibility scenarios against foundationdb-iceberg REST server.

Options:
  --fdb                     Start foundationdb-iceberg in FDB mode (-Dfdb=true).
  --no-start-server         Use existing server at REST_URI.
  --replace-server          If a local server is already using REST_URI's port, stop it and start a fresh one.
  --scenario list           Comma-separated scenario names to run.
  -h, --help                Show help.

Available scenarios:
  basic
  schema_evolution
  overwrite
  snapshots
  replace_table
  views

Environment overrides:
  SPARK_SQL_BIN             spark-sql binary path (default: spark-sql in PATH)
  SPARK_MASTER              Spark master (default: local[2])
  ICEBERG_HOME              Apache Iceberg repo path (default: /Users/dlambrig/iceberg)
  SPARK_VERSION             Spark line for runtime jar lookup (default: 3.5)
  SCALA_VERSION             Scala binary version for runtime jar lookup (default: 2.12)
  CATALOG_NAME              Spark catalog name (default: rest)
  WAREHOUSE_URI             Iceberg warehouse URI (default: file:///tmp/fdb_iceberg_spark_warehouse)
  REST_URI                  REST endpoint (default: http://localhost:8181)
  SCENARIOS                 Default scenario list if --scenario is omitted
  ICEBERG_RUNTIME_JAR       Explicit iceberg-spark-runtime jar path override
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
    --scenario)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --scenario requires a value" >&2
        exit 1
      fi
      SCENARIOS="$2"
      shift 2
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
SQL_FILES=()
cleanup() {
  set +e
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ ${#SQL_FILES[@]} -gt 0 ]]; then
    rm -f "${SQL_FILES[@]}" >/dev/null 2>&1 || true
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

spark_version_output() {
  "${SPARK_SQL_BIN}" --version 2>&1
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
    exit 1
  fi

  if [[ "${scala_version_line}" != *"Scala version ${SCALA_VERSION}."* ]]; then
    echo "ERROR: spark-sql Scala version does not match SCALA_VERSION=${SCALA_VERSION}" >&2
    echo "Detected: ${scala_version_line:-unknown}" >&2
    echo "Set SPARK_SQL_BIN to a Spark build using Scala ${SCALA_VERSION}.x." >&2
    exit 1
  fi
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

if ! command -v "${SPARK_SQL_BIN}" >/dev/null 2>&1; then
  echo "ERROR: spark-sql not found (${SPARK_SQL_BIN})." >&2
  echo "Install Spark or set SPARK_SQL_BIN to your spark-sql binary." >&2
  exit 1
fi

validate_spark_binary

ICEBERG_RUNTIME_JAR="${ICEBERG_RUNTIME_JAR:-}"
if [[ -z "${ICEBERG_RUNTIME_JAR}" ]]; then
  ICEBERG_RUNTIME_JAR="$(ls "${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark-runtime/build/libs"/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-*.jar 2>/dev/null | head -n1 || true)"
fi
if [[ -z "${ICEBERG_RUNTIME_JAR}" || ! -f "${ICEBERG_RUNTIME_JAR}" ]]; then
  echo "ERROR: Iceberg Spark runtime jar not found." >&2
  echo "Expected under: ${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark-runtime/build/libs/" >&2
  echo "Build it first, for example:" >&2
  echo "  cd ${ICEBERG_HOME}" >&2
  echo "  ./gradlew -DsparkVersions=${SPARK_VERSION} -DscalaVersion=${SCALA_VERSION} :iceberg-spark:iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}:jar" >&2
  echo "Or set ICEBERG_RUNTIME_JAR explicitly." >&2
  exit 1
fi

REST_PORT="$(rest_port)"
if [[ "${SERVER_MODE}" == "fdb" ]]; then
  require_fdb_prereqs
fi

echo "==> Spark smoke setup"
echo "Server mode: ${SERVER_MODE}"
echo "REST URI: ${REST_URI}"
echo "Spark binary: ${SPARK_SQL_BIN}"
echo "Iceberg runtime jar: ${ICEBERG_RUNTIME_JAR}"
echo "Warehouse: ${WAREHOUSE_URI}"
echo "Scenarios: ${SCENARIOS}"

if [[ "${START_SERVER}" == "true" ]]; then
  EXISTING_PIDS="$(list_port_pids "${REST_PORT}")"
  if [[ -n "${EXISTING_PIDS}" ]]; then
    if server_is_healthy; then
      echo "Reusing existing server at ${REST_URI} (PID(s): ${EXISTING_PIDS})"
      START_SERVER=false
    elif [[ "${REPLACE_SERVER}" == "true" ]]; then
      stop_port_pids "${REST_PORT}"
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
    if [[ "${SERVER_MODE}" == "fdb" ]]; then
      ./gradlew runIcebergRestServer "${SERVER_GRADLE_ARGS[@]}"
    else
      ./gradlew runIcebergRestServer
    fi
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
fi

wait_for_http "${REST_URI}/v1/config" "Server"

append_sql_file() {
  local scenario="$1"
  local sql_contents="$2"
  local sql_file="${LOG_DIR}/spark_${scenario}_${RUN_ID}.sql"
  printf "%s\n" "${sql_contents}" > "${sql_file}"
  SQL_FILES+=("${sql_file}")
  printf "%s\n" "${sql_file}"
}

run_spark_sql() {
  local scenario="$1"
  local sql_file="$2"
  echo "==> Running Spark scenario: ${scenario}" | tee -a "${SPARK_LOG}"
  "${SPARK_SQL_BIN}" \
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
    -f "${sql_file}" >> "${SPARK_LOG}" 2>&1
}

assert_log_contains() {
  local pattern="$1"
  local message="$2"
  if ! grep -Eq "${pattern}" "${SPARK_LOG}"; then
    echo "ERROR: ${message}" >&2
    echo "See log: ${SPARK_LOG}" >&2
    exit 1
  fi
}

run_basic_scenario() {
  local schema="spark_basic_${RUN_ID}"
  local table="orders"
  local sql_file
  sql_file="$(append_sql_file "basic" "CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${schema};
CREATE TABLE ${CATALOG_NAME}.${schema}.${table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (1, 10.5), (2, 20.25);
SELECT COUNT(*) AS basic_count FROM ${CATALOG_NAME}.${schema}.${table};
SELECT * FROM ${CATALOG_NAME}.${schema}.${table} ORDER BY order_id;
DROP TABLE ${CATALOG_NAME}.${schema}.${table};
DROP NAMESPACE ${CATALOG_NAME}.${schema};")"
  run_spark_sql "basic" "${sql_file}"
  assert_log_contains '(^|[[:space:]])2($|[[:space:]])' "Spark basic scenario did not produce COUNT(*) = 2."
  assert_log_contains '^1[[:space:]]+10\.5$' "Spark basic scenario did not print the first inserted row."
  assert_log_contains '^2[[:space:]]+20\.25$' "Spark basic scenario did not print the second inserted row."
}

run_schema_evolution_scenario() {
  local schema="spark_schema_${RUN_ID}"
  local table="orders"
  local sql_file
  sql_file="$(append_sql_file "schema_evolution" "CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${schema};
CREATE TABLE ${CATALOG_NAME}.${schema}.${table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (1, 10.5);
ALTER TABLE ${CATALOG_NAME}.${schema}.${table} ADD COLUMNS (note STRING);
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (2, 20.25, 'after alter');
SELECT COUNT(note) AS note_count FROM ${CATALOG_NAME}.${schema}.${table};
DESCRIBE TABLE ${CATALOG_NAME}.${schema}.${table};
DROP TABLE ${CATALOG_NAME}.${schema}.${table};
DROP NAMESPACE ${CATALOG_NAME}.${schema};")"
  run_spark_sql "schema_evolution" "${sql_file}"
  assert_log_contains '(^|[[:space:]])1($|[[:space:]])' "Spark schema evolution scenario did not produce COUNT(note) = 1."
  assert_log_contains 'note[[:space:]]+string' "Spark schema evolution scenario did not show the added note column."
}

run_overwrite_scenario() {
  local schema="spark_overwrite_${RUN_ID}"
  local table="orders"
  local sql_file
  sql_file="$(append_sql_file "overwrite" "CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${schema};
CREATE TABLE ${CATALOG_NAME}.${schema}.${table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (1, 10.5), (2, 20.25);
INSERT OVERWRITE ${CATALOG_NAME}.${schema}.${table} VALUES (9, 90.0);
SELECT COUNT(*) AS overwrite_count FROM ${CATALOG_NAME}.${schema}.${table};
SELECT MIN(order_id) AS min_order_id, MAX(order_id) AS max_order_id FROM ${CATALOG_NAME}.${schema}.${table};
DROP TABLE ${CATALOG_NAME}.${schema}.${table};
DROP NAMESPACE ${CATALOG_NAME}.${schema};")"
  run_spark_sql "overwrite" "${sql_file}"
  assert_log_contains '(^|[[:space:]])1($|[[:space:]])' "Spark overwrite scenario did not produce final COUNT(*) = 1."
  assert_log_contains '^9[[:space:]]+9$' "Spark overwrite scenario did not preserve only the overwritten row."
}

run_snapshots_scenario() {
  local schema="spark_snapshots_${RUN_ID}"
  local table="orders"
  local sql_file
  sql_file="$(append_sql_file "snapshots" "CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${schema};
CREATE TABLE ${CATALOG_NAME}.${schema}.${table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (1, 10.5);
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (2, 20.25);
SELECT COUNT(*) AS snapshot_count FROM ${CATALOG_NAME}.${schema}.${table}.snapshots;
SELECT COUNT(*) AS history_count FROM ${CATALOG_NAME}.${schema}.${table}.history;
DROP TABLE ${CATALOG_NAME}.${schema}.${table};
DROP NAMESPACE ${CATALOG_NAME}.${schema};")"
  run_spark_sql "snapshots" "${sql_file}"
  assert_log_contains '(^|[[:space:]])2($|[[:space:]])' "Spark snapshots scenario did not produce expected snapshot/history counts."
}

run_replace_table_scenario() {
  local schema="spark_replace_${RUN_ID}"
  local table="orders"
  local sql_file
  sql_file="$(append_sql_file "replace_table" "CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${schema};
CREATE TABLE ${CATALOG_NAME}.${schema}.${table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (1, 10.5), (2, 20.25);
REPLACE TABLE ${CATALOG_NAME}.${schema}.${table} USING iceberg AS SELECT CAST(9 AS BIGINT) AS order_id, CAST(90.0 AS DOUBLE) AS amount;
SELECT COUNT(*) AS replace_count FROM ${CATALOG_NAME}.${schema}.${table};
SELECT MIN(order_id) AS min_order_id, MAX(order_id) AS max_order_id FROM ${CATALOG_NAME}.${schema}.${table};
DROP TABLE ${CATALOG_NAME}.${schema}.${table};
DROP NAMESPACE ${CATALOG_NAME}.${schema};")"
  run_spark_sql "replace_table" "${sql_file}"
  assert_log_contains '(^|[[:space:]])1($|[[:space:]])' "Spark replace-table scenario did not produce final COUNT(*) = 1."
  assert_log_contains '^9[[:space:]]+9$' "Spark replace-table scenario did not preserve only the replacement row."
}

run_views_scenario() {
  local schema="spark_views_${RUN_ID}"
  local table="orders"
  local view="orders_v"
  local sql_file
  sql_file="$(append_sql_file "views" "CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${schema};
CREATE TABLE ${CATALOG_NAME}.${schema}.${table} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${schema}.${table} VALUES (1, 10.5), (2, 20.25);
CREATE VIEW ${CATALOG_NAME}.${schema}.${view} AS SELECT order_id, amount FROM ${CATALOG_NAME}.${schema}.${table};
SELECT COUNT(*) AS view_count FROM ${CATALOG_NAME}.${schema}.${view};
SHOW VIEWS IN ${CATALOG_NAME}.${schema};
DROP VIEW ${CATALOG_NAME}.${schema}.${view};
DROP TABLE ${CATALOG_NAME}.${schema}.${table};
DROP NAMESPACE ${CATALOG_NAME}.${schema};")"
  run_spark_sql "views" "${sql_file}"
  assert_log_contains '(^|[[:space:]])2($|[[:space:]])' "Spark view scenario did not produce COUNT(*) = 2 from the view."
  assert_log_contains "(^|[[:space:]])${view}([[:space:]]|$)" "Spark view scenario did not list the created view."
}

run_named_scenario() {
  local scenario="$1"
  case "${scenario}" in
    basic)
      run_basic_scenario
      ;;
    schema_evolution)
      run_schema_evolution_scenario
      ;;
    overwrite)
      run_overwrite_scenario
      ;;
    snapshots)
      run_snapshots_scenario
      ;;
    replace_table)
      run_replace_table_scenario
      ;;
    views)
      run_views_scenario
      ;;
    *)
      echo "ERROR: Unknown scenario: ${scenario}" >&2
      exit 1
      ;;
  esac
}

IFS=',' read -r -a scenario_list <<< "${SCENARIOS}"
if [[ ${#scenario_list[@]} -eq 0 ]]; then
  echo "ERROR: No Spark scenarios selected." >&2
  exit 1
fi

: > "${SPARK_LOG}"
for scenario in "${scenario_list[@]}"; do
  trimmed="$(printf '%s' "${scenario}" | tr -d '[:space:]')"
  if [[ -z "${trimmed}" ]]; then
    continue
  fi
  run_named_scenario "${trimmed}"
done

echo
echo "Spark scenarios passed against foundationdb-iceberg."
echo "Spark log: ${SPARK_LOG}"
if [[ "${START_SERVER}" == "true" ]]; then
  echo "Server log: ${SERVER_LOG}"
fi
