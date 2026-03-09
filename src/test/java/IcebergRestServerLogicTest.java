import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergRestServerLogicTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void buildLoadTableResponseJsonUsesLocalLocations() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"},
                {"id":2,"name":"amount","required":false,"type":"double"}
                ]}}
                """;

        String response = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        JsonNode root = MAPPER.readTree(response);

        String metadataLocation = root.path("metadata-location").asText();
        String tableLocation = root.path("metadata").path("location").asText();

        assertTrue(metadataLocation.startsWith("local:///iceberg_warehouse/_rest_metadata/"));
        assertEquals("local:///iceberg_warehouse/sales/orders", tableLocation);
        assertEquals(2, root.path("metadata").path("format-version").asInt());
    }

    @Test
    void applyCommitUpdatesSnapshotsRefsStatsAndSchema() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"},
                {"id":2,"name":"order_date","required":false,"type":"date"},
                {"id":3,"name":"amount","required":false,"type":"double"}
                ]}}
                """;

        String baseResponse = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String commit1 = """
                {
                  "updates": [
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":101,"type":"branch"},
                    {"action":"set-statistics","snapshot-id":101,"statistics":{"snapshot-id":101,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/stats-1"}}
                  ]
                }
                """;

        String afterCommit1 = IcebergRestServer.applyCommitToTableResponseJson(baseResponse, commit1);
        JsonNode metadata1 = MAPPER.readTree(afterCommit1).path("metadata");

        assertEquals(1, metadata1.path("last-sequence-number").asInt());
        assertEquals(101L, metadata1.path("current-snapshot-id").asLong());
        assertEquals(1, metadata1.path("snapshots").size());
        assertEquals(1, metadata1.path("statistics").size());
        assertEquals(1, metadata1.path("snapshot-log").size());

        String commit2 = """
                {
                  "updates": [
                    {"action":"add-schema","schema":{"type":"struct","schema-id":1,"fields":[
                      {"id":1,"name":"order_id","required":false,"type":"long"},
                      {"id":2,"name":"order_date","required":false,"type":"date"},
                      {"id":3,"name":"amount","required":false,"type":"double"},
                      {"id":4,"name":"note","required":false,"type":"string"}
                    ]},"last-column-id":4},
                    {"action":"set-current-schema","schema-id":-1}
                  ]
                }
                """;

        String afterCommit2 = IcebergRestServer.applyCommitToTableResponseJson(afterCommit1, commit2);
        JsonNode metadata2 = MAPPER.readTree(afterCommit2).path("metadata");

        assertEquals(4, metadata2.path("last-column-id").asInt());
        assertEquals(1, metadata2.path("current-schema-id").asInt());
        assertEquals(2, metadata2.path("schemas").size());
    }

    @Test
    void setCurrentSchemaWithExplicitIdWorksAndUnknownIdFails() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"}
                ]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String addSchema = """
                {
                  "updates": [
                    {"action":"add-schema","schema":{"type":"struct","schema-id":7,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]},"last-column-id":1},
                    {"action":"set-current-schema","schema-id":7}
                  ]
                }
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(base, addSchema);
        JsonNode metadata = MAPPER.readTree(updated).path("metadata");
        assertEquals(7, metadata.path("current-schema-id").asInt());

        String unknownSchema = """
                {"updates":[{"action":"set-current-schema","schema-id":999}]}
                """;
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(updated, unknownSchema));
        assertTrue(ex.getMessage().contains("unknown schema-id"));
    }

    @Test
    void commitRejectsUnknownActionAndMissingRequiredFields() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String unknownAction = """
                {"updates":[{"action":"do-something-else"}]}
                """;
        IllegalArgumentException unknown = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, unknownAction));
        assertTrue(unknown.getMessage().contains("Unsupported update action"));

        String missingSnapshotFields = """
                {"updates":[{"action":"add-snapshot","snapshot":{"timestamp-ms":1700000000000}}]}
                """;
        IllegalArgumentException missing = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, missingSnapshotFields));
        assertTrue(missing.getMessage().contains("snapshot-id"));
    }

    @Test
    void repeatedCommitWithSameRequirementConflicts() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String commit = """
                {
                  "requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
                  "updates":[
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":111,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":111,"type":"branch"}
                  ]
                }
                """;
        String once = IcebergRestServer.applyCommitToTableResponseJson(base, commit);
        assertEquals(111L, MAPPER.readTree(once).path("metadata").path("current-snapshot-id").asLong());

        IllegalStateException conflict = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(once, commit));
        assertTrue(conflict.getMessage().contains("assert-ref-snapshot-id"));
    }

    @Test
    void resolveWritablePathHandlesLocalAndFileSchemes() {
        Path localPath = IcebergRestServer.resolveWritablePath("local:///iceberg_warehouse/a/b/c.metadata.json");
        assertTrue(localPath.toString().contains("iceberg_warehouse"));

        Path expectedFilePath = Paths.get("/tmp/x.metadata.json");
        Path actualFilePath = IcebergRestServer.resolveWritablePath("file:///tmp/x.metadata.json");
        assertEquals(expectedFilePath, actualFilePath);
    }
}
