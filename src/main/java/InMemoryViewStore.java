import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class InMemoryViewStore implements ViewStore {
    private final Map<String, String> viewMetadataLocations = new HashMap<>();

    @Override
    public synchronized String getViewResponse(String namespace, String view) {
        String metadataLocation = viewMetadataLocations.get(key(namespace, view));
        if (metadataLocation == null) {
            return null;
        }
        return MetadataFileSupport.loadResponseFromMetadataLocation(metadataLocation);
    }

    @Override
    public synchronized void putViewResponse(String namespace, String view, String responseJson) {
        String metadataLocation = MetadataFileSupport.extractMetadataLocation(responseJson);
        viewMetadataLocations.put(key(namespace, view), metadataLocation);
    }

    @Override
    public synchronized String commitView(String namespace, String view, String commitRequestBody) {
        String currentMetadataLocation = viewMetadataLocations.get(key(namespace, view));
        if (currentMetadataLocation == null) {
            throw new ViewNotFoundException("View not found: " + namespace + "." + view);
        }
        String existingResponseJson = MetadataFileSupport.loadResponseFromMetadataLocation(currentMetadataLocation);
        String updatedResponseJson = IcebergRestServer.applyCommitToViewResponseJson(existingResponseJson, commitRequestBody);
        String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
        MetadataFileSupport.persistMetadataFile(updatedResponseJson);
        viewMetadataLocations.put(key(namespace, view), updatedMetadataLocation);
        return updatedResponseJson;
    }

    @Override
    public synchronized List<String> listViews(String namespace) {
        List<String> results = new ArrayList<>();
        String prefix = namespace + ".";
        for (String key : viewMetadataLocations.keySet()) {
            if (key.startsWith(prefix)) {
                results.add(key.substring(prefix.length()));
            }
        }
        Collections.sort(results);
        return results;
    }

    @Override
    public synchronized boolean deleteView(String namespace, String view) {
        return viewMetadataLocations.remove(key(namespace, view)) != null;
    }

    private static String key(String namespace, String view) {
        return namespace + "." + view;
    }
}
