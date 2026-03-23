import java.util.List;

class InMemoryViewStore extends AbstractInMemoryNamedMetadataStore implements ViewStore {

    @Override
    public synchronized String getViewResponse(String namespace, String view) {
        return getResponse(namespace, view);
    }

    @Override
    public synchronized void putViewResponse(String namespace, String view, String responseJson) {
        putResponse(namespace, view, responseJson);
    }

    @Override
    public synchronized String commitView(String namespace, String view, String commitRequestBody) {
        return commitViewResponse(namespace, view, commitRequestBody);
    }

    @Override
    public synchronized List<String> listViews(String namespace) {
        return listNames(namespace);
    }

    @Override
    public synchronized boolean deleteView(String namespace, String view) {
        return deleteName(namespace, view);
    }
}
