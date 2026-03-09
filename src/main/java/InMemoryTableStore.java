import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class InMemoryTableStore implements TableStore {
    private final Map<String, String> tables = new HashMap<>();

    @Override
    public synchronized String getTableResponse(String namespace, String table) {
        return tables.get(key(namespace, table));
    }

    @Override
    public synchronized void putTableResponse(String namespace, String table, String responseJson) {
        tables.put(key(namespace, table), responseJson);
    }

    @Override
    public synchronized List<String> listTables(String namespace) {
        List<String> results = new ArrayList<>();
        String prefix = namespace + ".";
        for (String key : tables.keySet()) {
            if (key.startsWith(prefix)) {
                results.add(key.substring(prefix.length()));
            }
        }
        Collections.sort(results);
        return results;
    }

    @Override
    public synchronized boolean deleteTable(String namespace, String table) {
        return tables.remove(key(namespace, table)) != null;
    }

    @Override
    public synchronized boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable) {
        String sourceKey = key(sourceNamespace, sourceTable);
        String targetKey = key(targetNamespace, targetTable);
        if (!tables.containsKey(sourceKey) || tables.containsKey(targetKey)) {
            return false;
        }
        tables.put(targetKey, tables.remove(sourceKey));
        return true;
    }

    private static String key(String namespace, String table) {
        return namespace + "." + table;
    }
}
