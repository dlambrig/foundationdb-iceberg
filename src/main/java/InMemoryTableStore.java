import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class InMemoryTableStore implements TableStore {
    private final Map<String, String> tableMetadataLocations = new HashMap<>();

    @Override
    public synchronized String getTableResponse(String namespace, String table) {
        String metadataLocation = tableMetadataLocations.get(key(namespace, table));
        if (metadataLocation == null) {
            return null;
        }
        return MetadataFileSupport.loadResponseFromMetadataLocation(metadataLocation);
    }

    @Override
    public synchronized void putTableResponse(String namespace, String table, String responseJson) {
        String metadataLocation = MetadataFileSupport.extractMetadataLocation(responseJson);
        tableMetadataLocations.put(key(namespace, table), metadataLocation);
    }

    @Override
    public synchronized String commitTable(String namespace, String table, String commitRequestBody) {
        String currentMetadataLocation = tableMetadataLocations.get(key(namespace, table));
        boolean tableExists = currentMetadataLocation != null;
        if (!tableExists && !IcebergRestServer.hasAssertCreateRequirement(commitRequestBody)) {
            throw new TableStore.TableNotFoundException("Table not found: " + namespace + "." + table);
        }

        String existingResponseJson = tableExists
                ? MetadataFileSupport.loadResponseFromMetadataLocation(currentMetadataLocation)
                : IcebergRestServer.buildEmptyTableResponseJson(namespace, table);
        String updatedResponseJson = IcebergRestServer.applyCommitToTableResponseJson(existingResponseJson, commitRequestBody, tableExists);
        List<String> metadataFilesToDelete = tableExists
                ? MetadataFileSupport.collectMetadataFilesToDeleteAfterCommit(existingResponseJson, updatedResponseJson)
                : List.of();
        String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
        MetadataFileSupport.persistMetadataFile(updatedResponseJson);
        tableMetadataLocations.put(key(namespace, table), updatedMetadataLocation);
        MetadataFileSupport.deleteMetadataFilesQuietly(metadataFilesToDelete);
        return updatedResponseJson;
    }

    @Override
    public synchronized List<String> listTables(String namespace) {
        List<String> results = new ArrayList<>();
        String prefix = namespace + ".";
        for (String key : tableMetadataLocations.keySet()) {
            if (key.startsWith(prefix)) {
                results.add(key.substring(prefix.length()));
            }
        }
        Collections.sort(results);
        return results;
    }

    @Override
    public synchronized boolean deleteTable(String namespace, String table) {
        return tableMetadataLocations.remove(key(namespace, table)) != null;
    }

    @Override
    public synchronized boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable) {
        String sourceKey = key(sourceNamespace, sourceTable);
        String targetKey = key(targetNamespace, targetTable);
        if (!tableMetadataLocations.containsKey(sourceKey) || tableMetadataLocations.containsKey(targetKey)) {
            return false;
        }
        tableMetadataLocations.put(targetKey, tableMetadataLocations.remove(sourceKey));
        return true;
    }

    private static String key(String namespace, String table) {
        return namespace + "." + table;
    }
}
