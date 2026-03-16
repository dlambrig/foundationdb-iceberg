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
- validates metrics endpoint behavior pre/post restart
- validates restart/reload behavior for both table and view metadata pointers
- validates namespace/table/view pagination behavior via REST APIs
- validates repeated concurrent writer conflict behavior (one commit succeeds, one conflicts per iteration)

Options:

```bash
./integration/run_fdb_integration.sh --no-smoke
./integration/run_fdb_integration.sh --no-start-server
```

Use already-running server on `:8181`:

```bash
./integration/trino_smoke.sh --no-start-server
```

Use already-running Trino on `:8080`:

```bash
./integration/trino_smoke.sh --no-start-trino
```

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
