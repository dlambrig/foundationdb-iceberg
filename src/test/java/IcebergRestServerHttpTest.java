import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergRestServerHttpTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        NamespaceStore ns = new InMemoryNamespaceStore(List.of("sales"));
        TableStore tables = new InMemoryTableStore();
        server = IcebergRestServer.startServer(0, ns, tables);
        baseUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void getConfigReturnsDefaults() throws Exception {
        HttpResponse response = request("GET", "/v1/config", null);
        assertEquals(200, response.statusCode);
        JsonNode root = MAPPER.readTree(response.body);
        assertTrue(root.has("defaults"));
        assertTrue(root.has("overrides"));
    }

    @Test
    void createAndListNamespaceWorks() throws Exception {
        HttpResponse create = request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        assertEquals(200, create.statusCode);

        HttpResponse list = request("GET", "/v1/namespaces", null);
        assertEquals(200, list.statusCode);
        JsonNode root = MAPPER.readTree(list.body);

        boolean found = false;
        for (JsonNode ns : root.path("namespaces")) {
            if (ns.isArray() && ns.size() == 1 && "analytics".equals(ns.get(0).asText())) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    @Test
    void unknownNamespaceReturnsIcebergErrorEnvelope() throws Exception {
        HttpResponse response = request("GET", "/v1/namespaces/does_not_exist", null);
        assertEquals(404, response.statusCode);
        JsonNode error = MAPPER.readTree(response.body).path("error");

        assertEquals(404, error.path("code").asInt());
        assertEquals("NoSuchEntityException", error.path("type").asText());
        assertTrue(error.path("message").asText().contains("Namespace not found"));
    }

    @Test
    void createTableRejectsMissingSchemaAndMalformedJson() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");

        HttpResponse missingSchema = request(
                "POST",
                "/v1/namespaces/analytics/tables",
                "{\"name\":\"orders\"}");
        assertEquals(400, missingSchema.statusCode);
        assertEquals(400, MAPPER.readTree(missingSchema.body).path("error").path("code").asInt());

        HttpResponse malformed = request(
                "POST",
                "/v1/namespaces/analytics/tables",
                "{\"name\":\"orders\"");
        assertEquals(400, malformed.statusCode);
        assertEquals(400, MAPPER.readTree(malformed.body).path("error").path("code").asInt());
    }

    @Test
    void createTableRejectsDuplicateTableName() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        HttpResponse duplicate = request("POST", "/v1/namespaces/analytics/tables", tableBody);
        assertEquals(409, duplicate.statusCode);
        assertEquals("CommitFailedException", MAPPER.readTree(duplicate.body).path("error").path("type").asText());
    }

    @Test
    void listTablesReturnsMultipleTables() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");

        String tableA = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        String tableB = "{\"name\":\"customers\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableA).statusCode);
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableB).statusCode);

        HttpResponse list = request("GET", "/v1/namespaces/analytics/tables", null);
        assertEquals(200, list.statusCode);
        JsonNode identifiers = MAPPER.readTree(list.body).path("identifiers");
        assertEquals(2, identifiers.size());
    }

    @Test
    void commitRejectsUnknownActionAndMissingFields() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        HttpResponse unknownAction = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"does-not-exist\"}]}");
        assertEquals(400, unknownAction.statusCode);

        HttpResponse missingFields = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\"}]}");
        assertEquals(400, missingFields.statusCode);

        HttpResponse malformedSetProperties = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"set-properties\",\"updates\":\"nope\"}]}");
        assertEquals(400, malformedSetProperties.statusCode);

        HttpResponse malformedRemoveProperties = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"remove-properties\",\"removals\":\"nope\"}]}");
        assertEquals(400, malformedRemoveProperties.statusCode);

        HttpResponse malformedSetLocation = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"set-location\",\"location\":\"not-a-uri\"}]}");
        assertEquals(400, malformedSetLocation.statusCode);

        HttpResponse malformedAddSpec = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"add-spec\",\"spec\":{\"spec-id\":1,\"fields\":\"nope\"}}]}");
        assertEquals(400, malformedAddSpec.statusCode);

        HttpResponse malformedAddSortOrder = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"add-sort-order\",\"sort-order\":{\"order-id\":1,\"fields\":[{\"source-id\":1,\"transform\":\"identity\",\"direction\":\"up\",\"null-order\":\"nulls-last\"}]}}]}");
        assertEquals(400, malformedAddSortOrder.statusCode);
    }

    @Test
    void commitRejectsUnknownRequirementTypeAndMalformedAssertRefPayload() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        HttpResponse unknownRequirement = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-unknown\",\"value\":1}],\"updates\":[]}");
        assertEquals(400, unknownRequirement.statusCode);

        HttpResponse malformedAssertRef = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"snapshot-id\":null}],\"updates\":[]}");
        assertEquals(400, malformedAssertRef.statusCode);

        HttpResponse assertCreate = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-create\"}],\"updates\":[]}");
        assertEquals(409, assertCreate.statusCode);

        HttpResponse assertPartitionConflict = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-last-assigned-partition-id\",\"last-assigned-partition-id\":1000}],\"updates\":[]}");
        assertEquals(409, assertPartitionConflict.statusCode);

        HttpResponse assertPartitionMalformed = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-last-assigned-partition-id\",\"last-assigned-partition-id\":\"abc\"}],\"updates\":[]}");
        assertEquals(400, assertPartitionMalformed.statusCode);

        HttpResponse assertDefaultSpecConflict = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-default-spec-id\",\"default-spec-id\":1}],\"updates\":[]}");
        assertEquals(409, assertDefaultSpecConflict.statusCode);

        HttpResponse assertDefaultSpecMalformed = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-default-spec-id\",\"default-spec-id\":\"abc\"}],\"updates\":[]}");
        assertEquals(400, assertDefaultSpecMalformed.statusCode);

        HttpResponse assertDefaultSortOrderConflict = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-default-sort-order-id\",\"default-sort-order-id\":1}],\"updates\":[]}");
        assertEquals(409, assertDefaultSortOrderConflict.statusCode);

        HttpResponse assertDefaultSortOrderMalformed = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-default-sort-order-id\",\"default-sort-order-id\":\"abc\"}],\"updates\":[]}");
        assertEquals(400, assertDefaultSortOrderMalformed.statusCode);
    }

    @Test
    void repeatedCommitReturnsConflict() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        String commit = """
                {
                  "requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
                  "updates":[
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":101,"type":"branch"}
                  ]
                }
                """;

        HttpResponse first = request("POST", "/v1/namespaces/analytics/tables/orders", commit);
        assertEquals(200, first.statusCode);

        HttpResponse second = request("POST", "/v1/namespaces/analytics/tables/orders", commit);
        assertEquals(409, second.statusCode);
        JsonNode err = MAPPER.readTree(second.body).path("error");
        assertEquals("CommitFailedException", err.path("type").asText());
    }

    @Test
    void notFoundErrorEnvelopeIsConsistentForNamespaceAndTableRoutes() throws Exception {
        HttpResponse missingNamespaceTableCreate = request(
                "POST",
                "/v1/namespaces/nope/tables",
                "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}}");
        assertEquals(404, missingNamespaceTableCreate.statusCode);
        assertEquals(404, MAPPER.readTree(missingNamespaceTableCreate.body).path("error").path("code").asInt());

        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        HttpResponse missingTableGet = request("GET", "/v1/namespaces/analytics/tables/unknown", null);
        assertEquals(404, missingTableGet.statusCode);
        assertEquals(404, MAPPER.readTree(missingTableGet.body).path("error").path("code").asInt());

        HttpResponse missingTableCommit = request("POST", "/v1/namespaces/analytics/tables/unknown", "{\"updates\":[]}");
        assertEquals(404, missingTableCommit.statusCode);
        assertEquals(404, MAPPER.readTree(missingTableCommit.body).path("error").path("code").asInt());
    }

    @Test
    void supportsNestedNamespaceListingWithParentFilter() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\",\"sales\"],\"properties\":{}}").statusCode);
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\",\"finance\"],\"properties\":{}}").statusCode);
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\",\"sales\",\"daily\"],\"properties\":{}}").statusCode);

        HttpResponse response = request("GET", "/v1/namespaces?parent=analytics", null);
        assertEquals(200, response.statusCode);
        JsonNode namespaces = MAPPER.readTree(response.body).path("namespaces");
        assertEquals(2, namespaces.size());
    }

    @Test
    void updateNamespacePropertiesWorks() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{\"owner\":\"data\"}}").statusCode);

        String updateBody = "{\"updates\":{\"tier\":\"gold\"},\"removals\":[\"owner\",\"missing_key\"]}";
        HttpResponse update = request("POST", "/v1/namespaces/analytics/properties", updateBody);
        assertEquals(200, update.statusCode);
        JsonNode updateJson = MAPPER.readTree(update.body);
        assertEquals(1, updateJson.path("updated").size());
        assertEquals(1, updateJson.path("removed").size());
        assertEquals(1, updateJson.path("missing").size());

        HttpResponse getNs = request("GET", "/v1/namespaces/analytics", null);
        assertEquals(200, getNs.statusCode);
        JsonNode props = MAPPER.readTree(getNs.body).path("properties");
        assertEquals("gold", props.path("tier").asText());
    }

    @Test
    void deleteTableAndNamespaceLifecycleWorks() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        HttpResponse namespaceDeleteConflict = request("DELETE", "/v1/namespaces/analytics", null);
        assertEquals(409, namespaceDeleteConflict.statusCode);

        HttpResponse deleteTable = request("DELETE", "/v1/namespaces/analytics/tables/orders", null);
        assertEquals(204, deleteTable.statusCode);

        HttpResponse deleteNamespace = request("DELETE", "/v1/namespaces/analytics", null);
        assertEquals(204, deleteNamespace.statusCode);
    }

    @Test
    void renameTableWorks() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        String renameBody = "{\"source\":{\"namespace\":[\"analytics\"],\"name\":\"orders\"},\"destination\":{\"namespace\":[\"analytics\"],\"name\":\"orders_new\"}}";
        HttpResponse rename = request("POST", "/v1/tables/rename", renameBody);
        assertEquals(204, rename.statusCode);

        assertEquals(404, request("GET", "/v1/namespaces/analytics/tables/orders", null).statusCode);
        assertEquals(200, request("GET", "/v1/namespaces/analytics/tables/orders_new", null).statusCode);
    }

    @Test
    void stagedCreateDoesNotPersistTable() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);
        String stagedBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]},\"stage-create\":true}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", stagedBody).statusCode);
        assertEquals(404, request("GET", "/v1/namespaces/analytics/tables/orders", null).statusCode);
    }

    @Test
    void getTableReflectsCommittedMetadataChanges() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        String snapshotCommit = """
                {
                  "requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
                  "updates":[
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":1001,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":1001,"type":"branch"},
                    {"action":"set-statistics","snapshot-id":1001,"statistics":{"snapshot-id":1001,"statistics-path":"local:///iceberg_warehouse/analytics/orders/metadata/stats-1001"}}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", snapshotCommit).statusCode);

        String schemaCommit = """
                {
                  "updates":[
                    {"action":"add-schema","schema":{"type":"struct","schema-id":2,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"note","required":false,"type":"string"}]},"last-column-id":2},
                    {"action":"set-current-schema","schema-id":2}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", schemaCommit).statusCode);

        String setPropertiesCommit = """
                {
                  "updates":[
                    {"action":"set-properties","updates":{"owner":"analytics-team","retention_days":"90"}}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", setPropertiesCommit).statusCode);

        String removePropertiesCommit = """
                {
                  "updates":[
                    {"action":"remove-properties","removals":["retention_days"]}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", removePropertiesCommit).statusCode);

        String setLocationCommit = """
                {
                  "updates":[
                    {"action":"set-location","location":"local:///iceberg_warehouse_custom/analytics/orders"}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", setLocationCommit).statusCode);

        String addSpecCommit = """
                {
                  "updates":[
                    {"action":"add-spec","spec":{"spec-id":1,"fields":[{"source-id":1,"field-id":1000,"name":"id_bucket","transform":"bucket[16]"}]}},
                    {"action":"set-default-spec","spec-id":1}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", addSpecCommit).statusCode);

        String addSortOrderCommit = """
                {
                  "updates":[
                    {"action":"add-sort-order","sort-order":{"order-id":1,"fields":[{"source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-last"}]}},
                    {"action":"set-default-sort-order","sort-order-id":1}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", addSortOrderCommit).statusCode);

        HttpResponse get = request("GET", "/v1/namespaces/analytics/tables/orders", null);
        assertEquals(200, get.statusCode);
        JsonNode metadata = MAPPER.readTree(get.body).path("metadata");

        assertEquals(1, metadata.path("last-sequence-number").asInt());
        assertEquals(1001L, metadata.path("current-snapshot-id").asLong());
        assertEquals(1001L, metadata.path("refs").path("main").path("snapshot-id").asLong());
        assertEquals(1, metadata.path("snapshots").size());
        assertEquals(1001L, metadata.path("snapshots").get(0).path("snapshot-id").asLong());
        assertEquals(1, metadata.path("statistics").size());
        assertEquals(1001L, metadata.path("statistics").get(0).path("snapshot-id").asLong());
        assertEquals(2, metadata.path("current-schema-id").asInt());
        assertEquals(2, metadata.path("last-column-id").asInt());
        assertEquals("local:///iceberg_warehouse_custom/analytics/orders", metadata.path("location").asText());
        assertEquals(1, metadata.path("default-spec-id").asInt());
        assertEquals(1000, metadata.path("last-partition-id").asInt());
        assertEquals(2, metadata.path("partition-specs").size());
        assertEquals(1, metadata.path("default-sort-order-id").asInt());
        assertEquals(2, metadata.path("sort-orders").size());
        assertEquals("analytics-team", metadata.path("properties").path("owner").asText());
        assertTrue(metadata.path("properties").path("retention_days").isMissingNode());
    }

    private HttpResponse request(String method, String path, String body) throws Exception {
        URL url = URI.create(baseUrl + path).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Content-Type", "application/json");

        if (body != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }

        int code = conn.getResponseCode();
        InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        String responseBody = "";
        if (is != null) {
            responseBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            is.close();
        }
        conn.disconnect();
        return new HttpResponse(code, responseBody);
    }

    private record HttpResponse(int statusCode, String body) {}
}
