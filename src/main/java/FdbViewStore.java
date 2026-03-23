import java.util.List;

class FdbViewStore extends AbstractFdbNamedMetadataStore implements ViewStore {
    FdbViewStore() {
        super("view");
    }

    @Override
    public String getViewResponse(String namespace, String view) {
        return getResponse(namespace, view);
    }

    @Override
    public void putViewResponse(String namespace, String view, String responseJson) {
        putResponse(namespace, view, responseJson);
    }

    @Override
    public String commitView(String namespace, String view, String commitRequestBody) {
        return commitViewResponse(namespace, view, commitRequestBody);
    }

    @Override
    public List<String> listViews(String namespace) {
        return listNames(namespace);
    }

    @Override
    public boolean deleteView(String namespace, String view) {
        return deleteName(namespace, view);
    }
}
