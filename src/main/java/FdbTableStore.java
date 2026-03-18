import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FdbTableStore implements TableStore {
    private final FDBDatabase database;

    FdbTableStore() {
        this.database = FDBDatabaseFactory.instance().getDatabase();
    }

    @Override
    public String getTableResponse(String namespace, String table) {
        String metadataLocation = database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "table", namespace, table).pack();
            byte[] value = context.ensureActive().get(key).join();
            if (value == null) {
                return null;
            }
            return new String(value, StandardCharsets.UTF_8);
        });
        if (metadataLocation == null) {
            return null;
        }
        return IcebergRestServer.loadTableResponseFromMetadataLocation(metadataLocation);
    }

    @Override
    public void putTableResponse(String namespace, String table, String responseJson) {
        String metadataLocation = IcebergRestServer.extractMetadataLocation(responseJson);
        database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "table", namespace, table).pack();
            context.ensureActive().set(key, metadataLocation.getBytes(StandardCharsets.UTF_8));
            return null;
        });
    }

    @Override
    public String commitTable(String namespace, String table, String commitRequestBody) {
        return database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "table", namespace, table).pack();
            byte[] existing = context.ensureActive().get(key).join();
            if (existing == null) {
                throw new TableStore.TableNotFoundException("Table not found: " + namespace + "." + table);
            }

            String currentMetadataLocation = new String(existing, StandardCharsets.UTF_8);
            String existingResponseJson = IcebergRestServer.loadTableResponseFromMetadataLocation(currentMetadataLocation);
            String updatedResponseJson = IcebergRestServer.applyCommitToTableResponseJson(existingResponseJson, commitRequestBody);
            List<String> metadataFilesToDelete = IcebergRestServer.collectMetadataFilesToDeleteAfterCommit(existingResponseJson, updatedResponseJson);
            String updatedMetadataLocation = IcebergRestServer.extractMetadataLocation(updatedResponseJson);
            IcebergRestServer.persistMetadataFile(updatedResponseJson);
            context.ensureActive().set(key, updatedMetadataLocation.getBytes(StandardCharsets.UTF_8));
            IcebergRestServer.deleteMetadataFilesQuietly(metadataFilesToDelete);
            return updatedResponseJson;
        });
    }

    @Override
    public List<String> listTables(String namespace) {
        return database.run(context -> {
            byte[] prefix = Tuple.from("iceberg-rest-server", "table", namespace).pack();
            List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefix)).asList().join();
            List<String> tables = new ArrayList<>();
            for (KeyValue row : rows) {
                Tuple keyTuple = Tuple.fromBytes(row.getKey());
                Object value = keyTuple.get(keyTuple.size() - 1);
                tables.add(String.valueOf(value));
            }
            Collections.sort(tables);
            return tables;
        });
    }

    @Override
    public boolean deleteTable(String namespace, String table) {
        return database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "table", namespace, table).pack();
            byte[] existing = context.ensureActive().get(key).join();
            if (existing == null) {
                return false;
            }
            context.ensureActive().clear(key);
            return true;
        });
    }

    @Override
    public boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable) {
        return database.run(context -> {
            byte[] sourceKey = Tuple.from("iceberg-rest-server", "table", sourceNamespace, sourceTable).pack();
            byte[] targetKey = Tuple.from("iceberg-rest-server", "table", targetNamespace, targetTable).pack();
            byte[] sourceValue = context.ensureActive().get(sourceKey).join();
            byte[] targetValue = context.ensureActive().get(targetKey).join();
            if (sourceValue == null || targetValue != null) {
                return false;
            }
            context.ensureActive().set(targetKey, sourceValue);
            context.ensureActive().clear(sourceKey);
            return true;
        });
    }
}
