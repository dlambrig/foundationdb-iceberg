# Integration Harness

This harness starts:
- `IcebergRestServer` (in-memory mode) on `:8181`
- Trino server pointed at your existing `~/trino/etc` config
- non-interactive SQL checks via `trino-cli --execute`

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
