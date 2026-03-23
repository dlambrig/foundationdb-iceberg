import java.util.List;

class FdbTableStore extends AbstractFdbNamedMetadataStore implements TableStore {
    FdbTableStore() {
        super("table");
    }

    @Override
    public String getTableResponse(String namespace, String table) {
        return getResponse(namespace, table);
    }

    @Override
    public void putTableResponse(String namespace, String table, String responseJson) {
        putResponse(namespace, table, responseJson);
    }

    @Override
    public String commitTable(String namespace, String table, String commitRequestBody) {
        return commitTableResponse(namespace, table, commitRequestBody);
    }

    @Override
    public List<String> listTables(String namespace) {
        return listNames(namespace);
    }

    @Override
    public boolean deleteTable(String namespace, String table) {
        return deleteName(namespace, table);
    }

    @Override
    public boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable) {
        return renameName(sourceNamespace, sourceTable, targetNamespace, targetTable);
    }
}
