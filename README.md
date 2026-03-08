# foundationdb-iceberg

Prototype Iceberg REST catalog work with:
- a Java mock REST server (`IcebergRestMockServer`)
- optional FoundationDB-backed metadata storage mode
- local Trino integration testing harness

## What Works So Far
- Basic Iceberg REST catalog endpoints used by Trino for:
  - catalog config
  - namespace list/create/get
  - table create/get/list
  - table commit updates (including snapshot and schema-evolution paths used in current tests)
  - metrics endpoint stub used by Trino
- Error responses are formatted for Iceberg REST clients.
- Integration harness can start mock + Trino, run SQL, and validate expected results.

## Run Mock Server
From repo root:

```bash
./gradlew runIcebergRestMock
```

FoundationDB mode:

```bash
./gradlew runIcebergRestMock --args="--fdb"
# or
./gradlew runIcebergRestMock -Dfdb=true
```

## Run Integration Test Harness

```bash
./integration/run_integration.sh
```

This script:
- starts the mock server
- starts Trino
- runs SQL checks for create/insert/read/snapshots/schema-evolution
- writes logs under `integration/logs/`

## Current Scope
This is a prototype, not a full production Iceberg REST catalog implementation yet.
