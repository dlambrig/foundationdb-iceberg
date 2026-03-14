import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class FdbMetricsStore implements MetricsStore {
    private final FDBDatabase database;

    FdbMetricsStore() {
        this.database = FDBDatabaseFactory.instance().getDatabase();
    }

    @Override
    public void recordTableMetrics(String namespace, String table, String reportType, String payloadJson) {
        database.run(context -> {
            long now = System.currentTimeMillis();
            String recordId = UUID.randomUUID().toString();
            byte[] key = Tuple.from("iceberg-rest-server", "table-metrics", namespace, table, now, recordId).pack();
            String value = reportType + "\n" + payloadJson;
            context.ensureActive().set(key, value.getBytes(StandardCharsets.UTF_8));
            return null;
        });
    }

    @Override
    public List<MetricRecord> listTableMetrics(String namespace, String table) {
        return database.run(context -> {
            byte[] prefix = Tuple.from("iceberg-rest-server", "table-metrics", namespace, table).pack();
            List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefix)).asList().join();
            List<MetricRecord> records = new ArrayList<>();
            for (KeyValue row : rows) {
                Tuple keyTuple = Tuple.fromBytes(row.getKey());
                long recordedAtMs = ((Number) keyTuple.get(4)).longValue();

                String value = new String(row.getValue(), StandardCharsets.UTF_8);
                int splitAt = value.indexOf('\n');
                String type;
                String payload;
                if (splitAt >= 0) {
                    type = value.substring(0, splitAt);
                    payload = value.substring(splitAt + 1);
                } else {
                    type = "unknown";
                    payload = value;
                }
                records.add(new MetricRecord(recordedAtMs, type, payload));
            }
            return records;
        });
    }
}
