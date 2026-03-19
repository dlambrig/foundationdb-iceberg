#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
SERVER_LOG="${LOG_DIR}/spark_server_${RUN_ID}.log"
SPARK_LOG="${LOG_DIR}/spark_smoke_${RUN_ID}.log"
SQL_FILE="${LOG_DIR}/spark_smoke_${RUN_ID}.sql"

START_SERVER=true
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

usage() {
  cat <<'EOF'
Usage: ./integration/spark_smoke.sh [--fdb] [--no-start-server]

Runs a Spark SQL smoke flow directly against foundationdb-iceberg REST server.

Options:
  --fdb              Start foundationdb-iceberg in FDB mode (-Dfdb=true).
  --no-start-server  Use existing server at REST_URI.
  -h, --help         Show help.

Environment overrides:
  SPARK_SQL_BIN      spark-sql binary path (default: spark-sql in PATH)
  SPARK_MASTER       Spark master (default: local[2])
  ICEBERG_HOME       Apache Iceberg repo path (default: /Users/dlambrig/iceberg)
  SPARK_VERSION      Spark line for runtime jar lookup (default: 3.5)
  SCALA_VERSION      Scala binary version for runtime jar lookup (default: 2.12)
  CATALOG_NAME       Spark catalog name (default: rest)
  WAREHOUSE_URI      Iceberg warehouse URI (default: file:///tmp/fdb_iceberg_spark_warehouse)
  REST_URI           REST endpoint (default: http://localhost:8181)
  ICEBERG_RUNTIME_JAR  Explicit iceberg-spark-runtime jar path override
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
  rm -f "${SQL_FILE}" >/dev/null 2>&1 || true
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

if [[ "${START_SERVER}" == "true" ]]; then
  require_port_free 8181 "Server"
fi
if [[ "${SERVER_MODE}" == "fdb" ]]; then
  require_fdb_prereqs
fi

echo "==> Spark smoke setup"
echo "Server mode: ${SERVER_MODE}"
echo "REST URI: ${REST_URI}"
echo "Spark binary: ${SPARK_SQL_BIN}"
echo "Iceberg runtime jar: ${ICEBERG_RUNTIME_JAR}"
echo "Warehouse: ${WAREHOUSE_URI}"

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

SCHEMA="spark_smoke_${RUN_ID}"
TABLE="orders"

cat > "${SQL_FILE}" <<EOF
CREATE NAMESPACE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA};
CREATE TABLE ${CATALOG_NAME}.${SCHEMA}.${TABLE} (order_id BIGINT, amount DOUBLE) USING iceberg;
INSERT INTO ${CATALOG_NAME}.${SCHEMA}.${TABLE} VALUES (1, 10.5), (2, 20.25);
SELECT COUNT(*) AS c FROM ${CATALOG_NAME}.${SCHEMA}.${TABLE};
SELECT * FROM ${CATALOG_NAME}.${SCHEMA}.${TABLE} ORDER BY order_id;
DROP TABLE ${CATALOG_NAME}.${SCHEMA}.${TABLE};
DROP NAMESPACE ${CATALOG_NAME}.${SCHEMA};
EOF

echo "==> Running Spark SQL smoke commands"
set -x
"${SPARK_SQL_BIN}" \
  --master "${SPARK_MASTER}" \
  --jars "${ICEBERG_RUNTIME_JAR}" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.type=rest" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.uri=${REST_URI}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_URI}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.hadoop.HadoopFileIO" \
  -f "${SQL_FILE}" | tee "${SPARK_LOG}"
set +x

if ! grep -Eq '(^|[[:space:]])2($|[[:space:]])' "${SPARK_LOG}"; then
  echo "ERROR: Spark smoke did not produce expected COUNT(*) = 2 output." >&2
  echo "See log: ${SPARK_LOG}" >&2
  exit 1
fi

echo
echo "Spark smoke passed against foundationdb-iceberg."
echo "Spark log: ${SPARK_LOG}"
if [[ "${START_SERVER}" == "true" ]]; then
  echo "Server log: ${SERVER_LOG}"
fi
