import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

class InMemoryNamespaceStore implements NamespaceStore {
    private final Set<String> namespaces = new LinkedHashSet<>();

    InMemoryNamespaceStore(List<String> initialNamespaces) {
        namespaces.addAll(initialNamespaces);
    }

    @Override
    public synchronized List<String> listNamespaces() {
        return new ArrayList<>(namespaces);
    }

    @Override
    public synchronized void createNamespace(String namespace) {
        namespaces.add(namespace);
    }

    @Override
    public synchronized boolean deleteNamespace(String namespace) {
        return namespaces.remove(namespace);
    }
}
