# Integration Harness

This harness starts:
- `IcebergRestServer` (in-memory mode) on `:8181`
- Trino server pointed at your existing `~/trino/etc` config
- non-interactive SQL checks via `trino-cli --execute`
- direct REST checks via `curl` (for commit requirement behavior like `assert-create`)
- metadata pointer transition checks (`metadata-location` advancing from `00000` to `00001`/`00002`)

For a faster compatibility subset, use `trino_smoke.sh` (minimal schema/table/insert/read/drop flow).

## Run

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/run_integration.sh
```

Run against FoundationDB-backed server mode:

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/run_integration.sh --fdb
```

## Run Fast Trino Smoke

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/trino_smoke.sh
```

By default this starts both:
- `IcebergRestServer` on `:8181`
- Trino on `:8080` (unless already running)
- a temporary copied Trino `etc/` with `local.location=/` so Trino can resolve absolute `file:///tmp/...` metadata paths correctly during metadata-table reads

Current direct Trino smoke coverage includes:
- schema/table lifecycle
- inserts and reads
- metadata tables (`$snapshots`, `$history`, `$files`, `$manifests`)
- schema evolution (`ADD COLUMN` + post-read validation)
- view lifecycle

FDB-backed mode:

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/trino_smoke.sh --fdb
```

## Run FDB-Focused Integration Checks

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/run_fdb_integration.sh
```

This script:
- runs `trino_smoke.sh --fdb` (unless `--no-smoke` is used)
- starts `IcebergRestServer` in FDB mode
- starts a fresh local Trino for direct FDB-backed checks (unless `--no-trino` is used)
- validates metrics endpoint behavior pre/post restart
- validates restart/reload behavior for both table and view metadata pointers
- validates direct Trino write/restart/read behavior against the FDB-backed catalog, including table/view reload and `$snapshots` after restart
- runs a direct Spark write/restart/read cycle against the FDB-backed catalog (unless `--no-spark` is used)
- validates namespace/table/view pagination behavior via REST APIs
- validates longer repeated concurrent writer conflict behavior (one commit succeeds, one conflicts per iteration)
- validates mixed update-action conflict behavior under concurrent commits
- validates restart during write cycles and commit continuity after restart

Options:

```bash
./integration/run_fdb_integration.sh --no-smoke
./integration/run_fdb_integration.sh --no-trino
./integration/run_fdb_integration.sh --no-spark
./integration/run_fdb_integration.sh --no-start-server
```

Stress knobs (optional env vars):

```bash
CONCURRENT_ITERATIONS=20
MIXED_CONFLICT_ITERATIONS=12
WRITE_CYCLE_ITERATIONS=18
```

Spark-specific overrides:

```bash
SPARK_SQL_BIN=~/spark-3.5.5/bin/spark-sql
ICEBERG_RUNTIME_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-*.jar
```

Use already-running server on `:8181`:

```bash
./integration/trino_smoke.sh --no-start-server
```

Force a fresh local catalog restart on `:8181`:

```bash
./integration/trino_smoke.sh --replace-server
```

Use already-running Trino on `:8080`:

```bash
./integration/trino_smoke.sh --no-start-trino
```

Force a fresh local Trino restart on `:8080`:

```bash
./integration/trino_smoke.sh --replace-trino
```

## Run Trino Connector Subset

Run a focused Trino Iceberg connector subset (REST-focused default classes):

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/run_trino_connector_subset.sh
```

Override tests:

```bash
./integration/run_trino_connector_subset.sh --tests "io.trino.plugin.iceberg.catalog.rest.TestIcebergRestCatalogSigV4Config"
```

Override Trino repo location:

```bash
./integration/run_trino_connector_subset.sh --trino-home /path/to/trino
```

Notes:
- Default subset includes Testcontainers-based smoke tests and expects Docker to be running.
- Logs are written under `integration/logs/trino_connector_subset_<timestamp>.log`.

## Run Spark Smoke (Against foundationdb-iceberg)

Run Spark SQL scenarios directly against this repo's REST server (`localhost:8181` by default):

```bash
cd /Users/dlambrig/apple/foundationdb-iceberg
./integration/spark_smoke.sh
```

FDB-backed server mode:

```bash
./integration/spark_smoke.sh --fdb
```

Use existing server:

```bash
./integration/spark_smoke.sh --no-start-server
```

Force the script to replace an unhealthy/conflicting local server on the same port:

```bash
./integration/spark_smoke.sh --replace-server
```

Run only selected scenarios:

```bash
./integration/spark_smoke.sh --scenario basic,schema_evolution
```

Current scenarios:
- `basic`: create namespace/table, insert, select, drop
- `schema_evolution`: add column, insert with new schema, verify `DESCRIBE`
- `overwrite`: append then `INSERT OVERWRITE`, verify final row set
- `snapshots`: multiple commits, then query Spark snapshot/history metadata tables
- `replace_table`: replace an existing table definition/data and verify the final row set
- `views`: create/query/list/drop a view through the REST catalog

Notes:
- This script requires `spark-sql`.
- It requires an Iceberg Spark runtime jar (`iceberg-spark-runtime-<spark>_<scala>-*.jar`), looked up by default under:
  - `~/iceberg/spark/v3.5/spark-runtime/build/libs/`
- You can override via `ICEBERG_RUNTIME_JAR`.
- If a healthy server is already listening at `REST_URI`, the script reuses it automatically.
- If a conflicting listener exists but is not a healthy Iceberg REST server, use `--replace-server` to restart it.
- Logs are written under `integration/logs/spark_smoke_<timestamp>.log`.
- Per-scenario SQL files are written under `integration/logs/` for the duration of the run.

## Prerequisites

- Trino source/build exists at `/Users/dlambrig/trino`
- Trino server distribution exists under:
  - `/Users/dlambrig/trino/core/trino-server/target/trino-server-*`
- Trino CLI executable jar exists under:
  - `/Users/dlambrig/trino/client/trino-cli/target/trino-cli-*-executable.jar`
- `~/trino/etc/catalog/iceberg.properties` points to:
  - `iceberg.rest-catalog.uri=http://localhost:8181`
- For `--fdb` mode:
  - `fdb.cluster` exists at repo root
  - FoundationDB client library exists (default expected path: `/usr/local/lib/libfdb_c.dylib`)
  - If needed, set `FDB_LIBRARY_PATH_FDB_C` to your `libfdb_c` path

Note: `trino_smoke.sh` can auto-start Trino if launcher/config are available; otherwise use `--no-start-trino` with an existing Trino instance.

## Environment Overrides

You can override defaults:

- `TRINO_HOME`
- `TRINO_ETC`
- `TRINO_SERVER_DIR`
- `TRINO_LAUNCHER`
- `TRINO_CLI_JAR`
- `TRINO_SERVER_URL`

Example:

```bash
TRINO_HOME=/path/to/trino TRINO_ETC=/path/to/etc ./integration/run_integration.sh
```

## Logs

Per-run logs are written to:

- `integration/logs/server_<timestamp>.log`
- `integration/logs/trino_<timestamp>.log`
