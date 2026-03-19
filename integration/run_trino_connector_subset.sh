#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TRINO_HOME="${TRINO_HOME:-/Users/dlambrig/trino}"
LOG_DIR="${PROJECT_ROOT}/integration/logs"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/trino_connector_subset_${RUN_ID}.log"

# Comma-separated Maven test pattern list.
DEFAULT_TESTS="io.trino.plugin.iceberg.catalog.rest.TestIcebergRestCatalogSigV4Config,io.trino.plugin.iceberg.catalog.rest.TestIcebergVendingRestCatalogConnectorSmokeTest,io.trino.plugin.iceberg.catalog.rest.TestIcebergUnityRestCatalogConnectorSmokeTest"
TESTS="${TRINO_TESTS:-${DEFAULT_TESTS}}"

usage() {
  cat <<'EOF'
Usage: ./integration/run_trino_connector_subset.sh [options]

Options:
  --trino-home PATH   Trino repo path (default: /Users/dlambrig/trino)
  --tests PATTERN     Comma-separated Maven -Dtest pattern override
  --no-docker-check   Skip docker daemon check
  -h, --help          Show help

Environment overrides:
  TRINO_HOME          Same as --trino-home
  TRINO_TESTS         Same as --tests

Default test subset:
  io.trino.plugin.iceberg.catalog.rest.TestIcebergRestCatalogSigV4Config
  io.trino.plugin.iceberg.catalog.rest.TestIcebergVendingRestCatalogConnectorSmokeTest
  io.trino.plugin.iceberg.catalog.rest.TestIcebergUnityRestCatalogConnectorSmokeTest
EOF
}

DOCKER_CHECK=true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --trino-home)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --trino-home requires a path" >&2
        exit 1
      fi
      TRINO_HOME="$2"
      shift 2
      ;;
    --tests)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --tests requires a value" >&2
        exit 1
      fi
      TESTS="$2"
      shift 2
      ;;
    --no-docker-check)
      DOCKER_CHECK=false
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

if [[ ! -d "${TRINO_HOME}" ]]; then
  echo "ERROR: TRINO_HOME does not exist: ${TRINO_HOME}" >&2
  exit 1
fi

if [[ ! -x "${TRINO_HOME}/mvnw" ]]; then
  echo "ERROR: Trino wrapper not found/executable: ${TRINO_HOME}/mvnw" >&2
  exit 1
fi

if [[ "${DOCKER_CHECK}" == "true" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is required for the default REST connector smoke tests." >&2
    echo "Install/start Docker, or pass --no-docker-check for custom non-container tests." >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon is not reachable." >&2
    echo "Start Docker Desktop/daemon and retry." >&2
    exit 1
  fi
fi

mkdir -p "${LOG_DIR}"

echo "==> Trino connector subset"
echo "TRINO_HOME: ${TRINO_HOME}"
echo "Tests: ${TESTS}"
echo "Log: ${LOG_FILE}"

set -x
(
  cd "${TRINO_HOME}"
  ./mvnw \
    -pl plugin/trino-iceberg \
    -Dair.check.skip-all=true \
    -DskipITs \
    -Dtest="${TESTS}" \
    test
) | tee "${LOG_FILE}"
set +x

echo "==> Done"
echo "Log written to: ${LOG_FILE}"
