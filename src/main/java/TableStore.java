import java.util.List;

interface TableStore {
    class TableNotFoundException extends RuntimeException {
        TableNotFoundException(String message) {
            super(message);
        }
    }

    String getTableResponse(String namespace, String table);

    void putTableResponse(String namespace, String table, String responseJson);

    String commitTable(String namespace, String table, String commitRequestBody);

    List<String> listTables(String namespace);

    boolean deleteTable(String namespace, String table);

    boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable);
}
