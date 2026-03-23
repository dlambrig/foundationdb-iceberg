import java.util.List;

class InMemoryTableStore extends AbstractInMemoryNamedMetadataStore implements TableStore {

    @Override
    public synchronized String getTableResponse(String namespace, String table) {
        return getResponse(namespace, table);
    }

    @Override
    public synchronized void putTableResponse(String namespace, String table, String responseJson) {
        putResponse(namespace, table, responseJson);
    }

    @Override
    public synchronized String commitTable(String namespace, String table, String commitRequestBody) {
        return commitTableResponse(namespace, table, commitRequestBody);
    }

    @Override
    public synchronized List<String> listTables(String namespace) {
        return listNames(namespace);
    }

    @Override
    public synchronized boolean deleteTable(String namespace, String table) {
        return deleteName(namespace, table);
    }

    @Override
    public synchronized boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable) {
        return renameName(sourceNamespace, sourceTable, targetNamespace, targetTable);
    }
}
