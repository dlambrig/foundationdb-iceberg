import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    void buildLoadViewResponseJsonUsesLocalLocations() throws Exception {
        String response = IcebergRestServer.buildLoadViewResponseJson(
                "sales",
                """
                {
                  "name":"orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]},
                  "properties":{"owner":"analytics"}
                }
                """);
        JsonNode root = MAPPER.readTree(response);

        String metadataLocation = root.path("metadata-location").asText();
        String viewLocation = root.path("metadata").path("location").asText();

        assertTrue(metadataLocation.startsWith("local:///iceberg_warehouse/_rest_metadata/"));
        assertEquals("local:///iceberg_warehouse/sales/views/orders_view", viewLocation);
        assertEquals(1, root.path("metadata").path("format-version").asInt());
        assertTrue(root.path("metadata").has("view-uuid"));
        assertEquals(1, root.path("metadata").path("current-version-id").asInt());
        assertEquals(1, root.path("metadata").path("versions").size());
        assertEquals(1, root.path("metadata").path("version-log").size());
        assertEquals("analytics", root.path("metadata").path("properties").path("owner").asText());
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
    void removeStatisticsRemovesOnlyMatchingSnapshotEntries() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String seeded = """
                {
                  "updates": [
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"add-snapshot","snapshot":{"sequence-number":2,"snapshot-id":202,"timestamp-ms":1700000001000,"schema-id":0}},
                    {"action":"set-statistics","statistics":{"snapshot-id":101,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/stats-101"}},
                    {"action":"set-statistics","statistics":{"snapshot-id":202,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/stats-202"}}
                  ]
                }
                """;
        String withStats = IcebergRestServer.applyCommitToTableResponseJson(base, seeded);

        String removeOne = """
                {"updates":[{"action":"remove-statistics","snapshot-ids":[202,999]}]}
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(withStats, removeOne);
        JsonNode statistics = MAPPER.readTree(updated).path("metadata").path("statistics");
        assertEquals(1, statistics.size());
        assertEquals(101L, statistics.get(0).path("snapshot-id").asLong());
    }

    @Test
    void setPartitionStatisticsValidatesSnapshotContextAndUpserts() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String withSnapshot = """
                {
                  "updates": [
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}}
                  ]
                }
                """;
        String seeded = IcebergRestServer.applyCommitToTableResponseJson(base, withSnapshot);

        String setPartitionStats = """
                {
                  "updates": [
                    {"action":"set-partition-statistics","partition-statistics":{"snapshot-id":101,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/part-stats-101-v1"}}
                  ]
                }
                """;
        String first = IcebergRestServer.applyCommitToTableResponseJson(seeded, setPartitionStats);
        JsonNode firstStats = MAPPER.readTree(first).path("metadata").path("partition-statistics");
        assertEquals(1, firstStats.size());
        assertEquals("local:///iceberg_warehouse/sales/orders/metadata/part-stats-101-v1", firstStats.get(0).path("statistics-path").asText());

        String upsertPartitionStats = """
                {
                  "updates": [
                    {"action":"set-partition-statistics","partition-statistics":{"snapshot-id":101,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/part-stats-101-v2"}}
                  ]
                }
                """;
        String second = IcebergRestServer.applyCommitToTableResponseJson(first, upsertPartitionStats);
        JsonNode secondStats = MAPPER.readTree(second).path("metadata").path("partition-statistics");
        assertEquals(1, secondStats.size());
        assertEquals("local:///iceberg_warehouse/sales/orders/metadata/part-stats-101-v2", secondStats.get(0).path("statistics-path").asText());

        String unknownSnapshot = """
                {
                  "updates": [
                    {"action":"set-partition-statistics","partition-statistics":{"snapshot-id":999,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/part-stats-999"}}
                  ]
                }
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(seeded, unknownSnapshot));
        assertTrue(ex1.getMessage().contains("unknown snapshot-id"));

        String unknownSchemaSnapshot = """
                {
                  "updates": [
                    {"action":"add-snapshot","snapshot":{"sequence-number":2,"snapshot-id":202,"timestamp-ms":1700000001000,"schema-id":7}},
                    {"action":"set-partition-statistics","partition-statistics":{"snapshot-id":202,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/part-stats-202"}}
                  ]
                }
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(seeded, unknownSchemaSnapshot));
        assertTrue(ex2.getMessage().contains("unknown schema-id"));
    }

    @Test
    void removePartitionStatisticsRemovesOnlyMatchingSnapshotEntriesAndValidatesPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String seeded = """
                {
                  "updates": [
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"add-snapshot","snapshot":{"sequence-number":2,"snapshot-id":202,"timestamp-ms":1700000001000,"schema-id":0}},
                    {"action":"set-partition-statistics","partition-statistics":{"snapshot-id":101,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/part-stats-101"}},
                    {"action":"set-partition-statistics","partition-statistics":{"snapshot-id":202,"statistics-path":"local:///iceberg_warehouse/sales/orders/metadata/part-stats-202"}}
                  ]
                }
                """;
        String withPartitionStats = IcebergRestServer.applyCommitToTableResponseJson(base, seeded);

        String removeOne = """
                {"updates":[{"action":"remove-partition-statistics","snapshot-ids":[202,999]}]}
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(withPartitionStats, removeOne);
        JsonNode partitionStats = MAPPER.readTree(updated).path("metadata").path("partition-statistics");
        assertEquals(1, partitionStats.size());
        assertEquals(101L, partitionStats.get(0).path("snapshot-id").asLong());

        String badPayload = """
                {"updates":[{"action":"remove-partition-statistics","snapshot-ids":"not-an-array"}]}
                """;
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withPartitionStats, badPayload));
        assertTrue(ex.getMessage().contains("requires snapshot-ids array"));
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

        String malformedSetProperties = """
                {"updates":[{"action":"set-properties","updates":"not-an-object"}]}
                """;
        IllegalArgumentException malformed = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, malformedSetProperties));
        assertTrue(malformed.getMessage().contains("set-properties requires updates object"));
    }

    @Test
    void setPropertiesUpdateMutatesTableProperties() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String setProps = """
                {"updates":[{"action":"set-properties","updates":{"owner":"data-platform","retention_days":"30"}}]}
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(base, setProps);
        JsonNode properties = MAPPER.readTree(updated).path("metadata").path("properties");

        assertEquals("data-platform", properties.path("owner").asText());
        assertEquals("30", properties.path("retention_days").asText());
    }

    @Test
    void removePropertiesUpdateRemovesKeysAndValidatesPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String seedProps = """
                {"updates":[{"action":"set-properties","updates":{"owner":"data-platform","retention_days":"30","env":"dev"}}]}
                """;
        String withProps = IcebergRestServer.applyCommitToTableResponseJson(base, seedProps);

        String removeProps = """
                {"updates":[{"action":"remove-properties","removals":["retention_days","missing_key"]}]}
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(withProps, removeProps);
        JsonNode properties = MAPPER.readTree(updated).path("metadata").path("properties");
        assertEquals("data-platform", properties.path("owner").asText());
        assertEquals("dev", properties.path("env").asText());
        assertTrue(properties.path("retention_days").isMissingNode());

        String malformed = """
                {"updates":[{"action":"remove-properties","removals":"not-an-array"}]}
                """;
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withProps, malformed));
        assertTrue(ex.getMessage().contains("remove-properties requires removals array"));
    }

    @Test
    void setLocationUpdateMutatesTableLocationAndValidatesPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String setLocation = """
                {"updates":[{"action":"set-location","location":"local:///iceberg_warehouse_custom/sales/orders"}]}
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(base, setLocation);
        JsonNode metadata = MAPPER.readTree(updated).path("metadata");
        assertEquals("local:///iceberg_warehouse_custom/sales/orders", metadata.path("location").asText());

        String malformed = """
                {"updates":[{"action":"set-location","location":"not-a-uri"}]}
                """;
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, malformed));
        assertTrue(ex.getMessage().contains("set-location requires valid URI location"));
    }

    @Test
    void addAndRemoveEncryptionKeyUpdatesMetadata() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String add = """
                {"updates":[{"action":"add-encryption-key","encryption-key":{"key-id":1,"wrapped-key":"abc","aad-prefix":"sales/orders"}}]}
                """;
        String withKey = IcebergRestServer.applyCommitToTableResponseJson(base, add);
        JsonNode keysAfterAdd = MAPPER.readTree(withKey).path("metadata").path("encryption-keys");
        assertEquals(1, keysAfterAdd.size());
        assertEquals(1L, keysAfterAdd.get(0).path("key-id").asLong());
        assertEquals("abc", keysAfterAdd.get(0).path("wrapped-key").asText());

        String remove = """
                {"updates":[{"action":"remove-encryption-key","key-id":1}]}
                """;
        String withoutKey = IcebergRestServer.applyCommitToTableResponseJson(withKey, remove);
        JsonNode keysAfterRemove = MAPPER.readTree(withoutKey).path("metadata").path("encryption-keys");
        assertEquals(0, keysAfterRemove.size());
    }

    @Test
    void encryptionKeyValidationRejectsBadPayloadsAndDuplicates() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String missingObject = """
                {"updates":[{"action":"add-encryption-key"}]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, missingObject));
        assertTrue(ex1.getMessage().contains("requires encryption-key object"));

        String missingKeyId = """
                {"updates":[{"action":"add-encryption-key","encryption-key":{"wrapped-key":"abc"}}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, missingKeyId));
        assertTrue(ex2.getMessage().contains("requires integer key-id"));

        String add = """
                {"updates":[{"action":"add-encryption-key","encryption-key":{"key-id":7,"wrapped-key":"abc"}}]}
                """;
        String withKey = IcebergRestServer.applyCommitToTableResponseJson(base, add);

        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withKey, add));
        assertTrue(ex3.getMessage().contains("key-id already exists"));

        String badRemove = """
                {"updates":[{"action":"remove-encryption-key","key-id":"x"}]}
                """;
        IllegalArgumentException ex4 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withKey, badRemove));
        assertTrue(ex4.getMessage().contains("key-id must be an integer"));
    }

    @Test
    void addSpecAndSetDefaultSpecUpdateMutatePartitionMetadata() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"},
                {"id":2,"name":"order_date","required":false,"type":"date"}
                ]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String addSpec = """
                {"updates":[{"action":"add-spec","spec":{"spec-id":1,"fields":[{"source-id":2,"field-id":1000,"name":"order_day","transform":"day"}]}}]}
                """;
        String afterAdd = IcebergRestServer.applyCommitToTableResponseJson(base, addSpec);
        JsonNode metadataAfterAdd = MAPPER.readTree(afterAdd).path("metadata");
        assertEquals(2, metadataAfterAdd.path("partition-specs").size());
        assertEquals(1000, metadataAfterAdd.path("last-partition-id").asInt());

        String setDefault = """
                {"updates":[{"action":"set-default-spec","spec-id":1}]}
                """;
        String afterDefault = IcebergRestServer.applyCommitToTableResponseJson(afterAdd, setDefault);
        JsonNode metadataAfterDefault = MAPPER.readTree(afterDefault).path("metadata");
        assertEquals(1, metadataAfterDefault.path("default-spec-id").asInt());

        String badAddSpec = """
                {"updates":[{"action":"add-spec","spec":{"spec-id":2,"fields":[{"source-id":2,"field-id":1001,"name":"x","transform":""}]}}]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterDefault, badAddSpec));
        assertTrue(ex1.getMessage().contains("requires source-id, field-id, name, and transform"));

        String badSetDefault = """
                {"updates":[{"action":"set-default-spec","spec-id":999}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterDefault, badSetDefault));
        assertTrue(ex2.getMessage().contains("unknown spec-id"));
    }

    @Test
    void removePartitionSpecsEnforcesDefaultAndInUseProtections() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"},
                {"id":2,"name":"order_date","required":false,"type":"date"},
                {"id":3,"name":"region","required":false,"type":"string"}
                ]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String seeded = """
                {
                  "updates": [
                    {"action":"add-spec","spec":{"spec-id":1,"fields":[{"source-id":2,"field-id":1000,"name":"order_day","transform":"day"}]}},
                    {"action":"add-spec","spec":{"spec-id":2,"fields":[{"source-id":3,"field-id":1001,"name":"region_id","transform":"identity"}]}},
                    {"action":"set-default-spec","spec-id":2},
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0,"spec-id":1}}
                  ]
                }
                """;
        String withSpecs = IcebergRestServer.applyCommitToTableResponseJson(base, seeded);

        String removeUnused = """
                {"updates":[{"action":"remove-partition-specs","spec-ids":[0]}]}
                """;
        String removed = IcebergRestServer.applyCommitToTableResponseJson(withSpecs, removeUnused);
        JsonNode specs = MAPPER.readTree(removed).path("metadata").path("partition-specs");
        assertEquals(2, specs.size());
        assertEquals(1, specs.get(0).path("spec-id").asInt());
        assertEquals(2, specs.get(1).path("spec-id").asInt());

        String removeDefault = """
                {"updates":[{"action":"remove-partition-specs","spec-ids":[2]}]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(removed, removeDefault));
        assertTrue(ex1.getMessage().contains("cannot remove default-spec-id"));

        String removeInUse = """
                {"updates":[{"action":"remove-partition-specs","spec-ids":[1]}]}
                """;
        IllegalStateException ex2 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(removed, removeInUse));
        assertTrue(ex2.getMessage().contains("cannot remove in-use spec-id"));
    }

    @Test
    void removeSchemasEnforcesCurrentAndInUseProtections() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String seeded = """
                {
                  "updates": [
                    {"action":"add-schema","schema":{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"},{"id":2,"name":"note","required":false,"type":"string"}]},"last-column-id":2},
                    {"action":"add-schema","schema":{"type":"struct","schema-id":2,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"},{"id":2,"name":"note","required":false,"type":"string"},{"id":3,"name":"region","required":false,"type":"string"}]},"last-column-id":3},
                    {"action":"set-current-schema","schema-id":2},
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":1}}
                  ]
                }
                """;
        String withSchemas = IcebergRestServer.applyCommitToTableResponseJson(base, seeded);

        String removeUnused = """
                {"updates":[{"action":"remove-schemas","schema-ids":[0]}]}
                """;
        String removed = IcebergRestServer.applyCommitToTableResponseJson(withSchemas, removeUnused);
        JsonNode schemas = MAPPER.readTree(removed).path("metadata").path("schemas");
        assertEquals(2, schemas.size());
        assertEquals(1, schemas.get(0).path("schema-id").asInt());
        assertEquals(2, schemas.get(1).path("schema-id").asInt());

        String removeCurrent = """
                {"updates":[{"action":"remove-schemas","schema-ids":[2]}]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(removed, removeCurrent));
        assertTrue(ex1.getMessage().contains("cannot remove current-schema-id"));

        String removeInUse = """
                {"updates":[{"action":"remove-schemas","schema-ids":[1]}]}
                """;
        IllegalStateException ex2 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(removed, removeInUse));
        assertTrue(ex2.getMessage().contains("cannot remove in-use schema-id"));
    }

    @Test
    void addSortOrderAndSetDefaultSortOrderMutateSortMetadata() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"},
                {"id":2,"name":"order_date","required":false,"type":"date"}
                ]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String addSortOrder = """
                {"updates":[{"action":"add-sort-order","sort-order":{"order-id":1,"fields":[{"source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-last"}]}}]}
                """;
        String afterAdd = IcebergRestServer.applyCommitToTableResponseJson(base, addSortOrder);
        JsonNode metadataAfterAdd = MAPPER.readTree(afterAdd).path("metadata");
        assertEquals(2, metadataAfterAdd.path("sort-orders").size());

        String setDefault = """
                {"updates":[{"action":"set-default-sort-order","sort-order-id":1}]}
                """;
        String afterDefault = IcebergRestServer.applyCommitToTableResponseJson(afterAdd, setDefault);
        JsonNode metadataAfterDefault = MAPPER.readTree(afterDefault).path("metadata");
        assertEquals(1, metadataAfterDefault.path("default-sort-order-id").asInt());

        String badDirection = """
                {"updates":[{"action":"add-sort-order","sort-order":{"order-id":2,"fields":[{"source-id":1,"transform":"identity","direction":"up","null-order":"nulls-last"}]}}]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterDefault, badDirection));
        assertTrue(ex1.getMessage().contains("direction must be asc or desc"));

        String badSourceId = """
                {"updates":[{"action":"add-sort-order","sort-order":{"order-id":2,"fields":[{"source-id":999,"transform":"identity","direction":"asc","null-order":"nulls-last"}]}}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterDefault, badSourceId));
        assertTrue(ex2.getMessage().contains("source-id does not exist"));
    }

    @Test
    void upgradeFormatVersionIsMonotonicAndValidatesPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        assertEquals(2, MAPPER.readTree(base).path("metadata").path("format-version").asInt());

        String upgrade = """
                {"updates":[{"action":"upgrade-format-version","format-version":3}]}
                """;
        String upgraded = IcebergRestServer.applyCommitToTableResponseJson(base, upgrade);
        assertEquals(3, MAPPER.readTree(upgraded).path("metadata").path("format-version").asInt());

        String sameVersion = """
                {"updates":[{"action":"upgrade-format-version","format-version":3}]}
                """;
        String same = IcebergRestServer.applyCommitToTableResponseJson(upgraded, sameVersion);
        assertEquals(3, MAPPER.readTree(same).path("metadata").path("format-version").asInt());

        String downgrade = """
                {"updates":[{"action":"upgrade-format-version","format-version":2}]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(upgraded, downgrade));
        assertTrue(ex1.getMessage().contains("cannot downgrade format-version"));

        String badPayload = """
                {"updates":[{"action":"upgrade-format-version","format-version":"abc"}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, badPayload));
        assertTrue(ex2.getMessage().contains("requires integer format-version"));
    }

    @Test
    void assignUuidIsWriteOnceAndIdempotent() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        String existingUuid = MAPPER.readTree(base).path("metadata").path("table-uuid").asText();
        assertTrue(!existingUuid.isEmpty());

        String sameUuid = """
                {"updates":[{"action":"assign-uuid","uuid":"%s"}]}
                """.formatted(existingUuid);
        String unchanged = IcebergRestServer.applyCommitToTableResponseJson(base, sameUuid);
        assertEquals(existingUuid, MAPPER.readTree(unchanged).path("metadata").path("table-uuid").asText());

        String differentUuid = """
                {"updates":[{"action":"assign-uuid","uuid":"00000000-0000-0000-0000-000000000001"}]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, differentUuid));
        assertTrue(ex1.getMessage().contains("cannot change existing table-uuid"));

        String invalidUuid = """
                {"updates":[{"action":"assign-uuid","uuid":"not-a-uuid"}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, invalidUuid));
        assertTrue(ex2.getMessage().contains("requires valid UUID"));
    }

    @Test
    void removeSnapshotRefSupportsValidRemovalAndMainProtection() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[
                {"id":1,"name":"order_id","required":false,"type":"long"}
                ]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String seedRefs = """
                {"updates":[
                  {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                  {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":101,"type":"branch"},
                  {"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":101,"type":"branch"}
                ]}
                """;
        String withRefs = IcebergRestServer.applyCommitToTableResponseJson(base, seedRefs);

        String removeDev = """
                {"updates":[{"action":"remove-snapshot-ref","ref-name":"dev"}]}
                """;
        String afterRemove = IcebergRestServer.applyCommitToTableResponseJson(withRefs, removeDev);
        JsonNode refs = MAPPER.readTree(afterRemove).path("metadata").path("refs");
        assertTrue(!refs.has("dev"));
        assertTrue(refs.has("main"));

        String removeMissing = """
                {"updates":[{"action":"remove-snapshot-ref","ref-name":"missing"}]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterRemove, removeMissing));
        assertTrue(ex1.getMessage().contains("unknown ref-name"));

        String removeMain = """
                {"updates":[{"action":"remove-snapshot-ref","ref-name":"main"}]}
                """;
        IllegalStateException ex2 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterRemove, removeMain));
        assertTrue(ex2.getMessage().contains("cannot remove required ref: main"));
    }

    @Test
    void removeSnapshotsRespectsRefAndParentInvariants() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String withSnapshots = """
                {"updates":[
                  {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                  {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":101,"type":"branch"},
                  {"action":"add-snapshot","snapshot":{"sequence-number":2,"snapshot-id":202,"parent-snapshot-id":101,"timestamp-ms":1700000001000,"schema-id":0}},
                  {"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":202,"type":"branch"}
                ]}
                """;
        String seeded = IcebergRestServer.applyCommitToTableResponseJson(base, withSnapshots);

        String removeProtected = """
                {"updates":[{"action":"remove-snapshots","snapshot-ids":[202]}]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(seeded, removeProtected));
        assertTrue(ex1.getMessage().contains("cannot remove referenced snapshot-id"));

        String removeDevRef = """
                {"updates":[{"action":"remove-snapshot-ref","ref-name":"dev"}]}
                """;
        String afterRefRemoval = IcebergRestServer.applyCommitToTableResponseJson(seeded, removeDevRef);

        String moveMainToChild = """
                {"updates":[{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":202,"type":"branch"}]}
                """;
        String afterMainMove = IcebergRestServer.applyCommitToTableResponseJson(afterRefRemoval, moveMainToChild);

        String removeParentWithChild = """
                {"updates":[{"action":"remove-snapshots","snapshot-ids":[101]}]}
                """;
        IllegalStateException ex2 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterMainMove, removeParentWithChild));
        assertTrue(ex2.getMessage().contains("dangling parent-snapshot-id"));

        String removeChild = """
                {"updates":[{"action":"remove-snapshots","snapshot-ids":[202]}]}
                """;
        String afterSnapshotRemoval = IcebergRestServer.applyCommitToTableResponseJson(afterRefRemoval, removeChild);
        JsonNode metadata = MAPPER.readTree(afterSnapshotRemoval).path("metadata");
        assertEquals(1, metadata.path("snapshots").size());
        assertEquals(101L, metadata.path("snapshots").get(0).path("snapshot-id").asLong());

        String unknownSnapshot = """
                {"updates":[{"action":"remove-snapshots","snapshot-ids":[999]}]}
                """;
        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(afterSnapshotRemoval, unknownSnapshot));
        assertTrue(ex3.getMessage().contains("unknown snapshot-id"));
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
    void commitRejectsUnknownRequirementType() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String commit = """
                {
                  "requirements":[{"type":"assert-unknown","value":"x"}],
                  "updates":[]
                }
                """;
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, commit));
        assertTrue(ex.getMessage().contains("Unsupported requirement type"));
    }

    @Test
    void assertRefSnapshotIdRejectsMalformedFields() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String missingRef = """
                {"requirements":[{"type":"assert-ref-snapshot-id","snapshot-id":null}],"updates":[]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, missingRef));
        assertTrue(ex1.getMessage().contains("non-empty ref"));

        String missingSnapshotId = """
                {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main"}],"updates":[]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, missingSnapshotId));
        assertTrue(ex2.getMessage().contains("requires snapshot-id"));

        String wrongSnapshotIdType = """
                {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":"abc"}],"updates":[]}
                """;
        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, wrongSnapshotIdType));
        assertTrue(ex3.getMessage().contains("must be an integer or null"));
    }

    @Test
    void assertCreateRequirementFailsOnExistingTableCommitPath() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String commit = """
                {
                  "requirements":[{"type":"assert-create"}],
                  "updates":[]
                }
                """;
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, commit));
        assertTrue(ex.getMessage().contains("assert-create failed"));
    }

    @Test
    void assertLastAssignedPartitionIdSupportsSuccessConflictAndBadPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String success = """
                {"requirements":[{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":999}],"updates":[]}
                """;
        String unchanged = IcebergRestServer.applyCommitToTableResponseJson(base, success);
        assertEquals(999L, MAPPER.readTree(unchanged).path("metadata").path("last-partition-id").asLong());

        String conflict = """
                {"requirements":[{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1000}],"updates":[]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, conflict));
        assertTrue(ex1.getMessage().contains("assert-last-assigned-partition-id failed"));

        String malformed = """
                {"requirements":[{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":"abc"}],"updates":[]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, malformed));
        assertTrue(ex2.getMessage().contains("requires integer last-assigned-partition-id"));
    }

    @Test
    void assertDefaultSpecIdSupportsSuccessConflictAndBadPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String success = """
                {"requirements":[{"type":"assert-default-spec-id","default-spec-id":0}],"updates":[]}
                """;
        String unchanged = IcebergRestServer.applyCommitToTableResponseJson(base, success);
        assertEquals(0L, MAPPER.readTree(unchanged).path("metadata").path("default-spec-id").asLong());

        String conflict = """
                {"requirements":[{"type":"assert-default-spec-id","default-spec-id":1}],"updates":[]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, conflict));
        assertTrue(ex1.getMessage().contains("assert-default-spec-id failed"));

        String malformed = """
                {"requirements":[{"type":"assert-default-spec-id","default-spec-id":"abc"}],"updates":[]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, malformed));
        assertTrue(ex2.getMessage().contains("requires integer default-spec-id"));
    }

    @Test
    void assertDefaultSortOrderIdSupportsSuccessConflictAndBadPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String success = """
                {"requirements":[{"type":"assert-default-sort-order-id","default-sort-order-id":0}],"updates":[]}
                """;
        String unchanged = IcebergRestServer.applyCommitToTableResponseJson(base, success);
        assertEquals(0L, MAPPER.readTree(unchanged).path("metadata").path("default-sort-order-id").asLong());

        String conflict = """
                {"requirements":[{"type":"assert-default-sort-order-id","default-sort-order-id":1}],"updates":[]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, conflict));
        assertTrue(ex1.getMessage().contains("assert-default-sort-order-id failed"));

        String malformed = """
                {"requirements":[{"type":"assert-default-sort-order-id","default-sort-order-id":"abc"}],"updates":[]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, malformed));
        assertTrue(ex2.getMessage().contains("requires integer default-sort-order-id"));
    }

    @Test
    void assertViewUuidSupportsSuccessConflictAndBadPayload() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        JsonNode baseRoot = MAPPER.readTree(base);
        ((com.fasterxml.jackson.databind.node.ObjectNode) baseRoot.path("metadata"))
                .put("view-uuid", "11111111-1111-1111-1111-111111111111");
        String withViewUuid = MAPPER.writeValueAsString(baseRoot);

        String success = """
                {"requirements":[{"type":"assert-view-uuid","uuid":"11111111-1111-1111-1111-111111111111"}],"updates":[]}
                """;
        String unchanged = IcebergRestServer.applyCommitToTableResponseJson(withViewUuid, success);
        assertEquals("11111111-1111-1111-1111-111111111111", MAPPER.readTree(unchanged).path("metadata").path("view-uuid").asText());

        String conflict = """
                {"requirements":[{"type":"assert-view-uuid","uuid":"22222222-2222-2222-2222-222222222222"}],"updates":[]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withViewUuid, conflict));
        assertTrue(ex1.getMessage().contains("assert-view-uuid failed"));

        String malformedMissing = """
                {"requirements":[{"type":"assert-view-uuid"}],"updates":[]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withViewUuid, malformedMissing));
        assertTrue(ex2.getMessage().contains("requires non-empty uuid"));

        String malformedBadUuid = """
                {"requirements":[{"type":"assert-view-uuid","uuid":"not-a-uuid"}],"updates":[]}
                """;
        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(withViewUuid, malformedBadUuid));
        assertTrue(ex3.getMessage().contains("requires valid UUID"));
    }

    @Test
    void applyCommitToViewResponseJsonSupportsVersionsAndProperties() throws Exception {
        String base = IcebergRestServer.buildLoadViewResponseJson(
                "sales",
                """
                {
                  "name":"orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]}
                }
                """);
        String viewUuid = MAPPER.readTree(base).path("metadata").path("view-uuid").asText();

        String commit = """
                {
                  "requirements":[{"type":"assert-view-uuid","uuid":"%s"}],
                  "updates":[
                    {"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000,"summary":{},"default-namespace":["sales"],"default-catalog":"prod","representations":[{"type":"sql","sql":"select 1","dialect":"trino"}]}},
                    {"action":"set-current-view-version","version-id":2},
                    {"action":"set-properties","updates":{"owner":"analytics-team"}},
                    {"action":"set-location","location":"local:///iceberg_warehouse_custom/sales/views/orders_view"}
                  ]
                }
                """.formatted(viewUuid);
        String updated = IcebergRestServer.applyCommitToViewResponseJson(base, commit);
        JsonNode metadata = MAPPER.readTree(updated).path("metadata");

        assertEquals(2, metadata.path("versions").size());
        assertEquals(2, metadata.path("version-log").size());
        assertEquals(2, metadata.path("current-version-id").asInt());
        assertEquals("analytics-team", metadata.path("properties").path("owner").asText());
        assertEquals("local:///iceberg_warehouse_custom/sales/views/orders_view", metadata.path("location").asText());
        assertEquals(1, metadata.path("metadata-log").size());
    }

    @Test
    void applyCommitToViewResponseJsonValidatesVersionActions() throws Exception {
        String base = IcebergRestServer.buildLoadViewResponseJson(
                "sales",
                """
                {
                  "name":"orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]}
                }
                """);

        String setUnknownVersion = """
                {"updates":[{"action":"set-current-view-version","version-id":42}]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, setUnknownVersion));
        assertTrue(ex1.getMessage().contains("unknown version-id"));

        String badAddVersion = """
                {"updates":[{"action":"add-view-version","view-version":{"schema-id":0}}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, badAddVersion));
        assertTrue(ex2.getMessage().contains("requires integer version-id"));

        String missingSchemaId = """
                {"updates":[{"action":"add-view-version","view-version":{"version-id":2,"timestamp-ms":1700000001000}}]}
                """;
        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, missingSchemaId));
        assertTrue(ex3.getMessage().contains("requires integer schema-id"));

        String missingTimestamp = """
                {"updates":[{"action":"add-view-version","view-version":{"version-id":2,"schema-id":0}}]}
                """;
        IllegalArgumentException ex4 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, missingTimestamp));
        assertTrue(ex4.getMessage().contains("requires integer timestamp-ms"));

        String missingSummary = """
                {"updates":[{"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000}}]}
                """;
        IllegalArgumentException ex5 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, missingSummary));
        assertTrue(ex5.getMessage().contains("requires summary object"));

        String badRepresentations = """
                {"updates":[{"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000,"summary":{},"default-namespace":["sales"],"representations":"bad"}}]}
                """;
        IllegalArgumentException ex6 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, badRepresentations));
        assertTrue(ex6.getMessage().contains("requires representations array"));

        String badDefaultNamespace = """
                {"updates":[{"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000,"summary":{},"default-namespace":[1],"representations":[{"type":"sql","sql":"select 1","dialect":"trino"}]}}]}
                """;
        IllegalArgumentException ex7 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, badDefaultNamespace));
        assertTrue(ex7.getMessage().contains("default-namespace entries"));

        String badSqlRepresentation = """
                {"updates":[{"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000,"summary":{},"default-namespace":["sales"],"representations":[{"type":"sql","sql":"","dialect":"trino"}]}}]}
                """;
        IllegalArgumentException ex8 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, badSqlRepresentation));
        assertTrue(ex8.getMessage().contains("requires non-empty sql"));
    }

    @Test
    void removeViewVersionsEnforcesCurrentAndUnknownProtections() throws Exception {
        String base = IcebergRestServer.buildLoadViewResponseJson(
                "sales",
                """
                {
                  "name":"orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]}
                }
                """);
        String withVersion2 = IcebergRestServer.applyCommitToViewResponseJson(
                base,
                """
                {
                  "updates":[
                    {"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000,"summary":{},"default-namespace":["sales"],"default-catalog":"prod","representations":[{"type":"sql","sql":"select 1","dialect":"trino"}]}},
                    {"action":"set-current-view-version","version-id":2}
                  ]
                }
                """);

        String removeOld = """
                {"updates":[{"action":"remove-view-versions","version-ids":[1]}]}
                """;
        String afterRemoveOld = IcebergRestServer.applyCommitToViewResponseJson(withVersion2, removeOld);
        JsonNode metadata = MAPPER.readTree(afterRemoveOld).path("metadata");
        assertEquals(1, metadata.path("versions").size());
        assertEquals(1, metadata.path("version-log").size());
        assertEquals(2, metadata.path("current-version-id").asInt());

        String removeCurrent = """
                {"updates":[{"action":"remove-view-versions","version-ids":[2]}]}
                """;
        IllegalStateException ex1 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(afterRemoveOld, removeCurrent));
        assertTrue(ex1.getMessage().contains("cannot remove current-version-id"));

        String removeUnknown = """
                {"updates":[{"action":"remove-view-versions","version-ids":[999]}]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(afterRemoveOld, removeUnknown));
        assertTrue(ex2.getMessage().contains("unknown version-id"));

        String malformed = """
                {"updates":[{"action":"remove-view-versions","version-ids":"bad"}]}
                """;
        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(afterRemoveOld, malformed));
        assertTrue(ex3.getMessage().contains("requires version-ids array"));
    }

    @Test
    void requirementValidationIsStrictForTypesAndConflictClassificationRemainsStable() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);

        String badCurrentSchemaType = """
                {"requirements":[{"type":"assert-current-schema-id","current-schema-id":"0"}],"updates":[]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, badCurrentSchemaType));
        assertTrue(ex1.getMessage().contains("requires integer current-schema-id"));

        String badDefaultSpecType = """
                {"requirements":[{"type":"assert-default-spec-id","default-spec-id":1.25}],"updates":[]}
                """;
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, badDefaultSpecType));
        assertTrue(ex2.getMessage().contains("requires integer default-spec-id"));

        String badTableUuidFormat = """
                {"requirements":[{"type":"assert-table-uuid","uuid":"not-a-uuid"}],"updates":[]}
                """;
        IllegalArgumentException ex3 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, badTableUuidFormat));
        assertTrue(ex3.getMessage().contains("requires valid UUID"));

        String nonStringRequirementType = """
                {"requirements":[{"type":123}],"updates":[]}
                """;
        IllegalArgumentException exType = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, nonStringRequirementType));
        assertTrue(exType.getMessage().contains("Requirement type is missing"));

        String assertCreateWithUnexpectedField = """
                {"requirements":[{"type":"assert-create","unexpected":true}],"updates":[]}
                """;
        IllegalArgumentException exShape1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, assertCreateWithUnexpectedField));
        assertTrue(exShape1.getMessage().contains("assert-create has unsupported field"));

        String assertRefSnapshotIdWithUnexpectedField = """
                {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null,"unexpected":"x"}],"updates":[]}
                """;
        IllegalArgumentException exShape2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, assertRefSnapshotIdWithUnexpectedField));
        assertTrue(exShape2.getMessage().contains("assert-ref-snapshot-id has unsupported field"));

        String conflictCurrentSchema = """
                {"requirements":[{"type":"assert-current-schema-id","current-schema-id":1}],"updates":[]}
                """;
        IllegalStateException ex4 = assertThrows(
                IllegalStateException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(base, conflictCurrentSchema));
        assertTrue(ex4.getMessage().contains("assert-current-schema-id failed"));
    }

    @Test
    void viewRequirementValidationRejectsUnknownFields() throws Exception {
        String base = IcebergRestServer.buildLoadViewResponseJson(
                "sales",
                """
                {
                  "name":"orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]}
                }
                """);

        String assertCreateWithUnexpectedField = """
                {"requirements":[{"type":"assert-create","extra":"x"}],"updates":[]}
                """;
        IllegalArgumentException ex1 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, assertCreateWithUnexpectedField));
        assertTrue(ex1.getMessage().contains("assert-create has unsupported field"));

        String viewUuid = MAPPER.readTree(base).path("metadata").path("view-uuid").asText();
        String assertViewUuidWithUnexpectedField = """
                {"requirements":[{"type":"assert-view-uuid","uuid":"%s","extra":"x"}],"updates":[]}
                """.formatted(viewUuid);
        IllegalArgumentException ex2 = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToViewResponseJson(base, assertViewUuidWithUnexpectedField));
        assertTrue(ex2.getMessage().contains("assert-view-uuid has unsupported field"));
    }

    @Test
    void inMemoryAtomicCommitAllowsOnlyOneWriterForSameRequirement() throws Exception {
        InMemoryTableStore store = new InMemoryTableStore();
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        IcebergRestServer.persistMetadataFile(base);
        store.putTableResponse("sales", "orders", base);

        String commit = """
                {
                  "requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
                  "updates":[
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":111,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":111,"type":"branch"}
                  ]
                }
                """;

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Callable<String>> tasks = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                tasks.add(() -> {
                    start.await(5, TimeUnit.SECONDS);
                    try {
                        store.commitTable("sales", "orders", commit);
                        return "success";
                    } catch (IllegalStateException e) {
                        return "conflict";
                    }
                });
            }
            List<Future<String>> futures = new ArrayList<>();
            for (Callable<String> task : tasks) {
                futures.add(pool.submit(task));
            }
            start.countDown();

            int success = 0;
            int conflict = 0;
            for (Future<String> future : futures) {
                String outcome = future.get(10, TimeUnit.SECONDS);
                if ("success".equals(outcome)) {
                    success++;
                } else if ("conflict".equals(outcome)) {
                    conflict++;
                }
            }

            assertEquals(1, success);
            assertEquals(1, conflict);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void commitRejectsMalformedMetadataLogChain() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        JsonNode baseRoot = MAPPER.readTree(base);
        ObjectNode rootObj = (ObjectNode) baseRoot;
        ObjectNode metadata = (ObjectNode) rootObj.path("metadata");
        ArrayNode metadataLog = metadata.putArray("metadata-log");
        metadataLog.addObject()
                .put("timestamp-ms", 1700000000000L)
                .put("metadata-file", rootObj.path("metadata-location").asText());

        String malformed = MAPPER.writeValueAsString(rootObj);
        String commit = "{\"updates\":[]}";
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> IcebergRestServer.applyCommitToTableResponseJson(malformed, commit));
        assertTrue(ex.getMessage().contains("metadata-log cannot contain current metadata-location"));
    }

    @Test
    void commitRepairsDuplicateMetadataLogEntriesAndAdvancesChain() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        JsonNode baseRoot = MAPPER.readTree(base);
        ObjectNode rootObj = (ObjectNode) baseRoot;
        String currentLocation = rootObj.path("metadata-location").asText();
        String uuid = currentLocation.replaceAll("^.*/\\d{5}-([^.]+)\\.metadata\\.json$", "$1");
        String v2 = currentLocation.replace("/00000-" + uuid + ".metadata.json", "/00002-" + uuid + ".metadata.json");
        rootObj.put("metadata-location", v2);

        ObjectNode metadata = (ObjectNode) rootObj.path("metadata");
        ArrayNode metadataLog = metadata.putArray("metadata-log");
        metadataLog.addObject().put("timestamp-ms", 1700000000000L).put("metadata-file",
                currentLocation.replace("/00000-" + uuid + ".metadata.json", "/00000-" + uuid + ".metadata.json"));
        metadataLog.addObject().put("timestamp-ms", 1700000001000L).put("metadata-file",
                currentLocation.replace("/00000-" + uuid + ".metadata.json", "/00001-" + uuid + ".metadata.json"));
        metadataLog.addObject().put("timestamp-ms", 1700000002000L).put("metadata-file",
                currentLocation.replace("/00000-" + uuid + ".metadata.json", "/00001-" + uuid + ".metadata.json"));

        String commit = "{\"updates\":[]}";
        String updated = IcebergRestServer.applyCommitToTableResponseJson(MAPPER.writeValueAsString(rootObj), commit);
        JsonNode updatedRoot = MAPPER.readTree(updated);
        JsonNode updatedLog = updatedRoot.path("metadata").path("metadata-log");
        assertEquals(3, updatedLog.size());
        assertEquals(
                currentLocation.replace("/00000-" + uuid + ".metadata.json", "/00002-" + uuid + ".metadata.json"),
                updatedLog.get(2).path("metadata-file").asText());
        assertTrue(updatedRoot.path("metadata-location").asText().contains("/00003-" + uuid + ".metadata.json"));
    }

    @Test
    void commitAdvancesMetadataPointerAndAppendsMetadataLog() throws Exception {
        String createRequest = """
                {"name":"orders","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":false,"type":"long"}]}}
                """;
        String base = IcebergRestServer.buildLoadTableResponseJson("sales", createRequest);
        JsonNode baseRoot = MAPPER.readTree(base);
        String initialLocation = baseRoot.path("metadata-location").asText();

        String commit = """
                {
                  "requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
                  "updates":[
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":111,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":111,"type":"branch"}
                  ]
                }
                """;
        String updated = IcebergRestServer.applyCommitToTableResponseJson(base, commit);
        JsonNode updatedRoot = MAPPER.readTree(updated);
        String nextLocation = updatedRoot.path("metadata-location").asText();

        assertTrue(initialLocation.endsWith(".metadata.json"));
        assertTrue(nextLocation.endsWith(".metadata.json"));
        assertTrue(nextLocation.contains("00001-"));
        assertTrue(!initialLocation.equals(nextLocation));

        JsonNode metadataLog = updatedRoot.path("metadata").path("metadata-log");
        assertEquals(1, metadataLog.size());
        assertEquals(initialLocation, metadataLog.get(0).path("metadata-file").asText());
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
