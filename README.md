# foundationdb-iceberg

Prototype Iceberg REST catalog work with:
- a Java REST server (`IcebergRestServer`)
- optional FoundationDB-backed metadata storage mode
- local Trino integration testing harness

## Requirements

### For server only
- macOS/Linux shell environment
- JDK 21+
- Gradle wrapper from this repo (`./gradlew`)

### For integration harness (`./integration/run_integration.sh`)
- Everything above, plus:
- Local Trino checkout/build available (set via `TRINO_HOME` if not default)
- Trino server launcher available under `core/trino-server/target/.../bin/.../launcher`
- Trino CLI jar available at `client/trino-cli/target/trino-cli-*-executable.jar`
- `curl` and `lsof` installed

### Optional for FoundationDB mode
- Running FoundationDB service
- `fdb.cluster` available locally
- FoundationDB client library (`libfdb_c`) available to the JVM when running FDB mode

Quick local option (Docker):

```bash
docker run -d --name fdb \
  -e FDB_NETWORKING_MODE=host \
  -e FDB_PORT=4550 \
  -e FDB_COORDINATOR_PORT=4550 \
  -p 4550:4550/tcp \
  foundationdb/foundationdb:7.3.38

# copy cluster file from container to your local project directory
docker cp fdb:/var/fdb/fdb.cluster ./fdb.cluster
```

## What Works So Far
- Basic Iceberg REST catalog endpoints used by Trino for:
  - catalog config
  - namespace list/create/get/delete
  - nested namespace creation and parent-filtered listing (`GET /v1/namespaces?parent=...`)
  - list pagination for namespaces/tables/views (`page-size`, `page-token`, `next-page-token`)
  - namespace properties updates (`POST /v1/namespaces/{ns}/properties`)
  - table create/get/list/delete
  - view create/get/list/delete
  - table rename (`POST /v1/tables/rename`)
  - staged create behavior (`stage-create=true` returns load response without persisting table)
  - table commit updates (including snapshot and schema-evolution paths used in current tests)
  - table metadata property updates via commit actions (`set-properties`, `remove-properties`)
  - partition spec updates via commit actions (`add-spec`, `set-default-spec`)
  - sort order updates via commit actions (`add-sort-order`, `set-default-sort-order`)
  - snapshot ref removal via commit action (`remove-snapshot-ref`, with `main` protected)
  - snapshot removal via commit action (`remove-snapshots`, with ref/parent safety checks)
  - table encryption key metadata updates via commit actions (`add-encryption-key`, `remove-encryption-key`)
  - view commit updates (`add-view-version`, `set-current-view-version`, `remove-view-versions`)
  - `assert-create` requirement handling on commit path (conflicts on existing tables)
  - `assert-last-assigned-partition-id` requirement validation
  - `assert-default-spec-id` requirement validation
  - `assert-default-sort-order-id` requirement validation
  - `assert-view-uuid` requirement validation
  - duplicate table create rejected with conflict (`409`)
  - strict validation for unknown commit requirement types and malformed `assert-ref-snapshot-id` payloads
  - table location updates via commit action (`set-location`)
  - metrics report ingestion endpoint (`POST /v1/namespaces/{ns}/tables/{table}/metrics`) with payload validation + persistence
- Error responses are formatted for Iceberg REST clients.
- Integration harness can start server + Trino, run SQL, and validate expected results.

## Run Server
From repo root:

```bash
./gradlew runIcebergRestServer
```

FoundationDB mode:

```bash
./gradlew runIcebergRestServer --args="--fdb"
# or
./gradlew runIcebergRestServer -Dfdb=true
```

## Run Integration Test Harness

```bash
./integration/run_integration.sh
```

This script:
- starts the server
- starts Trino
- runs SQL checks for create/insert/read/snapshots/schema-evolution/rename/drop lifecycle
- writes logs under `integration/logs/`

For a fast Trino compatibility subset (without full integration flow):

```bash
./integration/trino_smoke.sh
```

By default this script starts IcebergRestServer and Trino (if Trino is not already running).  
Use `--fdb` for FoundationDB mode, `--no-start-server` for an existing server on `localhost:8181`, or `--no-start-trino` for an existing Trino on `localhost:8080`.

## Current Scope
This is a prototype, not a full production Iceberg REST catalog implementation yet.

## RCK Status

As of March 18, 2026, the Iceberg Open API compatibility tests pass against this server:

```bash
cd ~/iceberg
./gradlew :iceberg-open-api:test
```

Latest observed result:
- 308 tests total
- 290 passed
- 18 skipped
- 0 failures
- 0 errors

## Spark Compatibility Note

- Running Spark tests inside `~/iceberg` is useful, but those tests usually use Iceberg's internal REST test harness by default.
- To validate `foundationdb-iceberg` directly, Spark must be configured with:
  - `type=rest`
  - `uri=http://localhost:8181` (or `http://host.docker.internal:8181` from container)
- Use this repo's direct Spark smoke script to test against your server:

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/spark_smoke.sh
```
