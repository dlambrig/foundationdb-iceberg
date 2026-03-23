import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class AbstractFdbNamedMetadataStore {
    private final String entityType;
    protected final FDBDatabase database;

    protected AbstractFdbNamedMetadataStore(String entityType) {
        this.entityType = entityType;
        this.database = FDBDatabaseFactory.instance().getDatabase();
    }

    protected String getResponse(String namespace, String name) {
        String metadataLocation = database.run(context -> {
            byte[] value = context.ensureActive().get(key(namespace, name)).join();
            if (value == null) {
                return null;
            }
            return new String(value, StandardCharsets.UTF_8);
        });
        if (metadataLocation == null) {
            return null;
        }
        return MetadataFileSupport.loadCatalogResponseFromMetadataLocation(metadataLocation);
    }

    protected void putResponse(String namespace, String name, String responseJson) {
        String metadataLocation = MetadataFileSupport.extractMetadataLocation(responseJson);
        database.run(context -> {
            context.ensureActive().set(key(namespace, name), metadataLocation.getBytes(StandardCharsets.UTF_8));
            return null;
        });
    }

    protected String commitTableResponse(String namespace, String table, String commitRequestBody) {
        return database.run(context -> {
            byte[] existing = context.ensureActive().get(key(namespace, table)).join();
            boolean tableExists = existing != null;
            if (!tableExists && !IcebergRestServer.hasAssertCreateRequirement(commitRequestBody)) {
                throw new TableStore.TableNotFoundException("Table not found: " + namespace + "." + table);
            }

            String existingResponseJson = tableExists
                    ? MetadataFileSupport.loadCatalogResponseFromMetadataLocation(new String(existing, StandardCharsets.UTF_8))
                    : IcebergRestServer.buildEmptyTableResponseJson(namespace, table);
            String updatedResponseJson = IcebergRestServer.applyCommitToTableResponseJson(existingResponseJson, commitRequestBody, tableExists);
            List<String> metadataFilesToDelete = tableExists
                    ? MetadataFileSupport.collectMetadataFilesToDeleteAfterCommit(existingResponseJson, updatedResponseJson)
                    : List.of();
            String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
            MetadataFileSupport.persistMetadataFile(updatedResponseJson);
            context.ensureActive().set(key(namespace, table), updatedMetadataLocation.getBytes(StandardCharsets.UTF_8));
            MetadataFileSupport.deleteMetadataFilesQuietly(metadataFilesToDelete);
            return updatedResponseJson;
        });
    }

    protected String commitViewResponse(String namespace, String view, String commitRequestBody) {
        return database.run(context -> {
            byte[] existing = context.ensureActive().get(key(namespace, view)).join();
            if (existing == null) {
                throw new ViewStore.ViewNotFoundException("View not found: " + namespace + "." + view);
            }

            String existingResponseJson = MetadataFileSupport.loadCatalogResponseFromMetadataLocation(new String(existing, StandardCharsets.UTF_8));
            String updatedResponseJson = IcebergRestServer.applyCommitToViewResponseJson(existingResponseJson, commitRequestBody);
            String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
            MetadataFileSupport.persistMetadataFile(updatedResponseJson);
            context.ensureActive().set(key(namespace, view), updatedMetadataLocation.getBytes(StandardCharsets.UTF_8));
            return updatedResponseJson;
        });
    }

    protected List<String> listNames(String namespace) {
        return database.run(context -> {
            byte[] prefix = Tuple.from("iceberg-rest-server", entityType, namespace).pack();
            List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefix)).asList().join();
            List<String> names = new ArrayList<>();
            for (KeyValue row : rows) {
                Tuple keyTuple = Tuple.fromBytes(row.getKey());
                names.add(String.valueOf(keyTuple.get(keyTuple.size() - 1)));
            }
            Collections.sort(names);
            return names;
        });
    }

    protected boolean deleteName(String namespace, String name) {
        return database.run(context -> {
            byte[] key = key(namespace, name);
            byte[] existing = context.ensureActive().get(key).join();
            if (existing == null) {
                return false;
            }
            context.ensureActive().clear(key);
            return true;
        });
    }

    protected boolean renameName(String sourceNamespace, String sourceName, String targetNamespace, String targetName) {
        return database.run(context -> {
            byte[] sourceKey = key(sourceNamespace, sourceName);
            byte[] targetKey = key(targetNamespace, targetName);
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

    private byte[] key(String namespace, String name) {
        return Tuple.from("iceberg-rest-server", entityType, namespace, name).pack();
    }
}
