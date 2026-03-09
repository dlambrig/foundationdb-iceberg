import java.util.List;

interface TableStore {
    String getTableResponse(String namespace, String table);

    void putTableResponse(String namespace, String table, String responseJson);

    List<String> listTables(String namespace);

    boolean deleteTable(String namespace, String table);

    boolean renameTable(String sourceNamespace, String sourceTable, String targetNamespace, String targetTable);
}
