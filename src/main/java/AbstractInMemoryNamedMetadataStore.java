import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class AbstractInMemoryNamedMetadataStore {
    private final Map<String, String> metadataLocations = new HashMap<>();

    protected synchronized String getResponse(String namespace, String name) {
        String metadataLocation = metadataLocations.get(key(namespace, name));
        if (metadataLocation == null) {
            return null;
        }
        return MetadataFileSupport.loadCatalogResponseFromMetadataLocation(metadataLocation);
    }

    protected synchronized void putResponse(String namespace, String name, String responseJson) {
        metadataLocations.put(key(namespace, name), MetadataFileSupport.extractMetadataLocation(responseJson));
    }

    protected synchronized String commitTableResponse(String namespace, String table, String commitRequestBody) {
        String currentMetadataLocation = metadataLocations.get(key(namespace, table));
        boolean tableExists = currentMetadataLocation != null;
        if (!tableExists && !IcebergRestServer.hasAssertCreateRequirement(commitRequestBody)) {
            throw new TableStore.TableNotFoundException("Table not found: " + namespace + "." + table);
        }

        String existingResponseJson = tableExists
                ? MetadataFileSupport.loadCatalogResponseFromMetadataLocation(currentMetadataLocation)
                : IcebergRestServer.buildEmptyTableResponseJson(namespace, table);
        String updatedResponseJson = IcebergRestServer.applyCommitToTableResponseJson(existingResponseJson, commitRequestBody, tableExists);
        List<String> metadataFilesToDelete = tableExists
                ? MetadataFileSupport.collectMetadataFilesToDeleteAfterCommit(existingResponseJson, updatedResponseJson)
                : List.of();
        String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
        MetadataFileSupport.persistMetadataFile(updatedResponseJson);
        metadataLocations.put(key(namespace, table), updatedMetadataLocation);
        MetadataFileSupport.deleteMetadataFilesQuietly(metadataFilesToDelete);
        return updatedResponseJson;
    }

    protected synchronized String commitViewResponse(String namespace, String view, String commitRequestBody) {
        String currentMetadataLocation = metadataLocations.get(key(namespace, view));
        if (currentMetadataLocation == null) {
            throw new ViewStore.ViewNotFoundException("View not found: " + namespace + "." + view);
        }

        String existingResponseJson = MetadataFileSupport.loadCatalogResponseFromMetadataLocation(currentMetadataLocation);
        String updatedResponseJson = IcebergRestServer.applyCommitToViewResponseJson(existingResponseJson, commitRequestBody);
        String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
        MetadataFileSupport.persistMetadataFile(updatedResponseJson);
        metadataLocations.put(key(namespace, view), updatedMetadataLocation);
        return updatedResponseJson;
    }

    protected synchronized List<String> listNames(String namespace) {
        List<String> results = new ArrayList<>();
        String prefix = namespace + ".";
        for (String key : metadataLocations.keySet()) {
            if (key.startsWith(prefix)) {
                results.add(key.substring(prefix.length()));
            }
        }
        Collections.sort(results);
        return results;
    }

    protected synchronized boolean deleteName(String namespace, String name) {
        return metadataLocations.remove(key(namespace, name)) != null;
    }

    protected synchronized boolean renameName(String sourceNamespace, String sourceName, String targetNamespace, String targetName) {
        String sourceKey = key(sourceNamespace, sourceName);
        String targetKey = key(targetNamespace, targetName);
        if (!metadataLocations.containsKey(sourceKey) || metadataLocations.containsKey(targetKey)) {
            return false;
        }
        metadataLocations.put(targetKey, metadataLocations.remove(sourceKey));
        return true;
    }

    private static String key(String namespace, String name) {
        return namespace + "." + name;
    }
}
