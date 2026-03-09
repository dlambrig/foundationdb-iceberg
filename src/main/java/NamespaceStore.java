import java.util.List;

interface NamespaceStore {
    List<String> listNamespaces();

    void createNamespace(String namespace);

    boolean deleteNamespace(String namespace);
}
