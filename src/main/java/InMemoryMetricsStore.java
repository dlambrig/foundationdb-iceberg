import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class InMemoryMetricsStore implements MetricsStore {
    private final Map<String, List<MetricRecord>> metrics = new HashMap<>();

    @Override
    public synchronized void recordTableMetrics(String namespace, String table, String reportType, String payloadJson) {
        String key = key(namespace, table);
        List<MetricRecord> existing = metrics.computeIfAbsent(key, ignored -> new ArrayList<>());
        existing.add(new MetricRecord(System.currentTimeMillis(), reportType, payloadJson));
    }

    @Override
    public synchronized List<MetricRecord> listTableMetrics(String namespace, String table) {
        return new ArrayList<>(metrics.getOrDefault(key(namespace, table), List.of()));
    }

    private static String key(String namespace, String table) {
        return namespace + "." + table;
    }
}
