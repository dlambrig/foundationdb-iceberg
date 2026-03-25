# Architecture

This document describes the current code structure of `foundationdb-iceberg` using a C4-style view in Mermaid.

## System Context

```mermaid
flowchart TB
    trino[Trino]
    spark[Spark]
    flink[Flink]
    other[Other Iceberg REST Clients]

    catalog[foundationdb-iceberg REST Catalog]
    fdb[(FoundationDB)]
    files[(Metadata/Data Files\nfile:// or object storage)]

    trino --> catalog
    spark --> catalog
    flink --> catalog
    other --> catalog

    catalog --> fdb
    catalog --> files
```

## Container View

```mermaid
flowchart TB
    clients[Iceberg Clients]

    subgraph catalog_system[foundationdb-iceberg]
        server[IcebergRestServer\nHTTP API + request handling]
        ns[NamespaceStore]
        tbl[TableStore]
        vw[ViewStore]
        metrics[MetricsStore]
        meta[MetadataFileSupport]
    end

    fdb[(FoundationDB)]
    files[(Metadata/Data Files)]

    clients --> server
    server --> ns
    server --> tbl
    server --> vw
    server --> metrics
    server --> meta

    ns --> fdb
    tbl --> fdb
    vw --> fdb
    metrics --> fdb

    tbl --> meta
    vw --> meta
    meta --> files
```

## Component View

```mermaid
flowchart TB
    subgraph server[IcebergRestServer]
        routes[HTTP route handling]
        validation[Request validation\nand error mapping]
        commits[Table/View commit semantics]
        paging[Namespace/Table/View\npagination helpers]
        config[Config + capability\nadvertisement]
    end

    subgraph stores[Store Interfaces]
        ns_api[NamespaceStore]
        tbl_api[TableStore]
        vw_api[ViewStore]
        metrics_api[MetricsStore]
    end

    subgraph memory[In-Memory Implementations]
        ns_mem[InMemoryNamespaceStore]
        tbl_mem[InMemoryTableStore]
        vw_mem[InMemoryViewStore]
        metrics_mem[InMemoryMetricsStore]
        mem_named[AbstractInMemoryNamedMetadataStore]
    end

    subgraph fdb_impl[FoundationDB Implementations]
        ns_fdb[FdbNamespaceStore]
        tbl_fdb[FdbTableStore]
        vw_fdb[FdbViewStore]
        metrics_fdb[FdbMetricsStore]
        fdb_named[AbstractFdbNamedMetadataStore]
    end

    subgraph metadata[Shared Metadata Helpers]
        file_support[MetadataFileSupport]
    end

    routes --> validation
    routes --> commits
    routes --> paging
    routes --> config

    server --> ns_api
    server --> tbl_api
    server --> vw_api
    server --> metrics_api

    ns_api --> ns_mem
    ns_api --> ns_fdb

    tbl_api --> tbl_mem
    tbl_api --> tbl_fdb
    vw_api --> vw_mem
    vw_api --> vw_fdb
    metrics_api --> metrics_mem
    metrics_api --> metrics_fdb

    tbl_mem --> mem_named
    vw_mem --> mem_named
    tbl_fdb --> fdb_named
    vw_fdb --> fdb_named

    commits --> file_support
    mem_named --> file_support
    fdb_named --> file_support
```

## Code Mapping

- `src/main/java/IcebergRestServer.java`
  - HTTP entrypoint
  - request parsing/validation
  - commit semantics
  - pagination and error handling

- `src/main/java/MetadataFileSupport.java`
  - metadata-location parsing
  - metadata file persistence/loading
  - metadata file cleanup
  - location/path resolution

- `src/main/java/NamespaceStore.java`
  - namespace abstraction

- `src/main/java/TableStore.java`
  - table abstraction

- `src/main/java/ViewStore.java`
  - view abstraction

- `src/main/java/MetricsStore.java`
  - metrics abstraction

- `src/main/java/AbstractInMemoryNamedMetadataStore.java`
  - shared in-memory table/view metadata behavior

- `src/main/java/AbstractFdbNamedMetadataStore.java`
  - shared FDB-backed table/view metadata behavior

- `src/main/java/InMemory*Store.java`
  - default local/dev backend

- `src/main/java/Fdb*Store.java`
  - FoundationDB-backed backend

## Notes

- The REST server stores namespace/table/view indexes in the selected backing store.
- Metadata JSON is persisted separately from the index entries and loaded through `MetadataFileSupport`.
- Memory mode and FDB mode share the same REST and commit logic; they differ mainly in how namespace/table/view/metrics indexes are stored.
