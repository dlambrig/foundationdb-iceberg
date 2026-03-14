import java.util.List;

interface MetricsStore {
    record MetricRecord(long recordedAtMs, String reportType, String payloadJson) {}

    void recordTableMetrics(String namespace, String table, String reportType, String payloadJson);

    List<MetricRecord> listTableMetrics(String namespace, String table);
}
