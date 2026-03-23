import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FdbViewStore implements ViewStore {
    private final FDBDatabase database;

    FdbViewStore() {
        this.database = FDBDatabaseFactory.instance().getDatabase();
    }

    @Override
    public String getViewResponse(String namespace, String view) {
        String metadataLocation = database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "view", namespace, view).pack();
            byte[] value = context.ensureActive().get(key).join();
            if (value == null) {
                return null;
            }
            return new String(value, StandardCharsets.UTF_8);
        });
        if (metadataLocation == null) {
            return null;
        }
        return MetadataFileSupport.loadResponseFromMetadataLocation(metadataLocation);
    }

    @Override
    public void putViewResponse(String namespace, String view, String responseJson) {
        String metadataLocation = MetadataFileSupport.extractMetadataLocation(responseJson);
        database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "view", namespace, view).pack();
            context.ensureActive().set(key, metadataLocation.getBytes(StandardCharsets.UTF_8));
            return null;
        });
    }

    @Override
    public String commitView(String namespace, String view, String commitRequestBody) {
        return database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "view", namespace, view).pack();
            byte[] existing = context.ensureActive().get(key).join();
            if (existing == null) {
                throw new ViewStore.ViewNotFoundException("View not found: " + namespace + "." + view);
            }

            String currentMetadataLocation = new String(existing, StandardCharsets.UTF_8);
            String existingResponseJson = MetadataFileSupport.loadResponseFromMetadataLocation(currentMetadataLocation);
            String updatedResponseJson = IcebergRestServer.applyCommitToViewResponseJson(existingResponseJson, commitRequestBody);
            String updatedMetadataLocation = MetadataFileSupport.extractMetadataLocation(updatedResponseJson);
            MetadataFileSupport.persistMetadataFile(updatedResponseJson);
            context.ensureActive().set(key, updatedMetadataLocation.getBytes(StandardCharsets.UTF_8));
            return updatedResponseJson;
        });
    }

    @Override
    public List<String> listViews(String namespace) {
        return database.run(context -> {
            byte[] prefix = Tuple.from("iceberg-rest-server", "view", namespace).pack();
            List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefix)).asList().join();
            List<String> views = new ArrayList<>();
            for (KeyValue row : rows) {
                Tuple keyTuple = Tuple.fromBytes(row.getKey());
                Object value = keyTuple.get(keyTuple.size() - 1);
                views.add(String.valueOf(value));
            }
            Collections.sort(views);
            return views;
        });
    }

    @Override
    public boolean deleteView(String namespace, String view) {
        return database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "view", namespace, view).pack();
            byte[] existing = context.ensureActive().get(key).join();
            if (existing == null) {
                return false;
            }
            context.ensureActive().clear(key);
            return true;
        });
    }
}
