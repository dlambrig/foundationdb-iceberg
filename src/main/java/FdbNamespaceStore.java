import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FdbNamespaceStore implements NamespaceStore {
    private static final Tuple PREFIX_TUPLE = Tuple.from("iceberg-rest-server", "namespace");
    private final FDBDatabase database;
    private final byte[] prefixBytes;

    FdbNamespaceStore() {
        this.database = FDBDatabaseFactory.instance().getDatabase();
        this.prefixBytes = PREFIX_TUPLE.pack();
    }

    @Override
    public List<String> listNamespaces() {
        return database.run(context -> {
            List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefixBytes)).asList().join();
            List<String> namespaces = new ArrayList<>();
            for (KeyValue row : rows) {
                Tuple keyTuple = Tuple.fromBytes(row.getKey());
                Object value = keyTuple.get(keyTuple.size() - 1);
                namespaces.add(String.valueOf(value));
            }
            Collections.sort(namespaces);
            return namespaces;
        });
    }

    @Override
    public void createNamespace(String namespace) {
        database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "namespace", namespace).pack();
            context.ensureActive().set(key, new byte[] {1});
            return null;
        });
    }

    @Override
    public boolean deleteNamespace(String namespace) {
        return database.run(context -> {
            byte[] key = Tuple.from("iceberg-rest-server", "namespace", namespace).pack();
            byte[] existing = context.ensureActive().get(key).join();
            if (existing == null) {
                return false;
            }
            context.ensureActive().clear(key);
            return true;
        });
    }
}
