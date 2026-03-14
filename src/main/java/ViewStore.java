import java.util.List;

interface ViewStore {
    String getViewResponse(String namespace, String view);

    void putViewResponse(String namespace, String view, String responseJson);

    String commitView(String namespace, String view, String commitRequestBody);

    List<String> listViews(String namespace);

    boolean deleteView(String namespace, String view);

    class ViewNotFoundException extends RuntimeException {
        ViewNotFoundException(String message) {
            super(message);
        }
    }
}
