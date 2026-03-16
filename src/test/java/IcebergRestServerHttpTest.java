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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergRestServerHttpTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private String baseUrl;
    private InMemoryMetricsStore metricsStore;

    @BeforeEach
    void setUp() throws Exception {
        NamespaceStore ns = new InMemoryNamespaceStore(List.of("sales"));
        TableStore tables = new InMemoryTableStore();
        metricsStore = new InMemoryMetricsStore();
        server = IcebergRestServer.startServer(0, ns, tables, new InMemoryViewStore(), metricsStore);
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
    void errorEnvelopeMatrixAcrossEndpointsIsConsistent() throws Exception {
        assertErrorEnvelope("POST", "/v1/config", "{}", 405, "RESTException", "Method Not Allowed");

        assertErrorEnvelope("POST", "/v1/namespaces", "{\"namespace\":[],\"properties\":{}}", 400, "BadRequestException", "Invalid namespace");
        assertErrorEnvelope("POST", "/v1/namespaces", "{\"namespace\":[\"n1\"],\"properties\":\"bad\"}", 400, "BadRequestException", "Invalid properties");

        assertErrorEnvelope("GET", "/v1/does-not-exist", null, 404, "NoSuchEntityException", "Not Found");

        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);

        assertErrorEnvelope("POST", "/v1/namespaces/analytics/properties", "{\"updates\":{},\"removals\":\"bad\"}", 400, "BadRequestException", "Invalid properties update payload");
        assertErrorEnvelope("POST", "/v1/tables/rename", "{\"source\":{},\"destination\":{}}", 400, "BadRequestException", "Invalid rename payload");
        assertErrorEnvelope("POST", "/v1/tables/rename", "{\"source\":{\"namespace\":[\"analytics\"],\"name\":\"a\"},\"destination\":{\"namespace\":[\"missing\"],\"name\":\"b\"}}", 404, "NoSuchEntityException", "Namespace not found");

        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        assertErrorEnvelope("POST", "/v1/namespaces/analytics/tables/orders", "{\"updates\":[{\"action\":\"unknown\"}]}", 400, "BadRequestException", "Unsupported update action");
        assertErrorEnvelope("POST", "/v1/namespaces/analytics/tables/orders", "{\"requirements\":[{\"type\":\"assert-create\"}],\"updates\":[]}", 409, "CommitFailedException", "assert-create failed");
        assertErrorEnvelope("GET", "/v1/namespaces/analytics/tables/missing", null, 404, "NoSuchEntityException", "Table not found");

        String viewBody = """
                {"name":"orders_v","view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[]}}
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views", viewBody).statusCode);
        assertErrorEnvelope("POST", "/v1/namespaces/analytics/views", "{\"name\":\"bad\"}", 400, "BadRequestException", "Missing or invalid view-version");
        assertErrorEnvelope("POST", "/v1/namespaces/analytics/views/orders_v", "{\"updates\":[{\"action\":\"set-current-view-version\",\"version-id\":999}]}", 400, "BadRequestException", "unknown version-id");
        assertErrorEnvelope("GET", "/v1/namespaces/analytics/views/missing", null, 404, "NoSuchEntityException", "View not found");

        assertErrorEnvelope("GET", "/v1/namespaces?page-size=0", null, 400, "BadRequestException", "Invalid page-size");
        assertErrorEnvelope("GET", "/v1/namespaces?page-token=missing", null, 400, "BadRequestException", "Invalid page-token");
    }

    @Test
    void internalServerErrorsUseConsistentEnvelope() throws Exception {
        server.stop(0);
        AtomicBoolean firstListCall = new AtomicBoolean(true);
        NamespaceStore brokenNs = new NamespaceStore() {
            @Override
            public List<String> listNamespaces() {
                if (firstListCall.getAndSet(false)) {
                    return List.of();
                }
                throw new RuntimeException("boom");
            }

            @Override
            public void createNamespace(String namespace) {
                throw new RuntimeException("boom");
            }

            @Override
            public boolean deleteNamespace(String namespace) {
                throw new RuntimeException("boom");
            }
        };
        server = IcebergRestServer.startServer(0, brokenNs, new InMemoryTableStore(), new InMemoryViewStore());
        baseUrl = "http://localhost:" + server.getAddress().getPort();

        HttpResponse response = request("GET", "/v1/namespaces", null);
        assertEquals(500, response.statusCode);
        JsonNode error = MAPPER.readTree(response.body).path("error");
        assertEquals(500, error.path("code").asInt());
        assertEquals("ServerErrorException", error.path("type").asText());
        assertTrue(error.path("message").asText().contains("Internal Server Error"));
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
    void tablePaginationReturnsNextPageTokenAndContinuation() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableA = "{\"name\":\"a\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        String tableB = "{\"name\":\"b\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        String tableC = "{\"name\":\"c\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableA).statusCode);
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableB).statusCode);
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableC).statusCode);

        HttpResponse page1 = request("GET", "/v1/namespaces/analytics/tables?page-size=2", null);
        assertEquals(200, page1.statusCode);
        JsonNode page1Json = MAPPER.readTree(page1.body);
        assertEquals(2, page1Json.path("identifiers").size());
        assertEquals("b", page1Json.path("next-page-token").asText());

        HttpResponse page2 = request("GET", "/v1/namespaces/analytics/tables?page-size=2&page-token=b", null);
        assertEquals(200, page2.statusCode);
        JsonNode page2Json = MAPPER.readTree(page2.body);
        assertEquals(1, page2Json.path("identifiers").size());
        assertEquals("c", page2Json.path("identifiers").get(0).path("name").asText());
        assertTrue(page2Json.path("next-page-token").isMissingNode());
    }

    @Test
    void createListCommitAndDeleteViewWorks() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");

        String createViewBody = """
                {
                  "name":"orders_view",
                  "location":"local:///iceberg_warehouse/analytics/views/orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"order_id","required":true,"type":"long"}]},
                  "properties":{"owner":"analytics-team"}
                }
                """;
        HttpResponse create = request("POST", "/v1/namespaces/analytics/views", createViewBody);
        assertEquals(200, create.statusCode);
        JsonNode createRoot = MAPPER.readTree(create.body);
        assertTrue(createRoot.path("metadata").has("view-uuid"));
        assertEquals(1, createRoot.path("metadata").path("current-version-id").asInt());

        HttpResponse list = request("GET", "/v1/namespaces/analytics/views", null);
        assertEquals(200, list.statusCode);
        JsonNode identifiers = MAPPER.readTree(list.body).path("identifiers");
        assertEquals(1, identifiers.size());
        assertEquals("orders_view", identifiers.get(0).path("name").asText());

        String commitBody = """
                {
                  "requirements":[{"type":"assert-view-uuid","uuid":"%s"}],
                  "updates":[
                    {"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000}},
                    {"action":"set-current-view-version","version-id":2},
                    {"action":"set-properties","updates":{"owner":"analytics-team"}}
                  ]
                }
                """.formatted(createRoot.path("metadata").path("view-uuid").asText());
        HttpResponse commit = request("POST", "/v1/namespaces/analytics/views/orders_view", commitBody);
        assertEquals(200, commit.statusCode);
        JsonNode committedMetadata = MAPPER.readTree(commit.body).path("metadata");
        assertEquals(2, committedMetadata.path("versions").size());
        assertEquals(2, committedMetadata.path("current-version-id").asInt());
        assertEquals("analytics-team", committedMetadata.path("properties").path("owner").asText());

        HttpResponse get = request("GET", "/v1/namespaces/analytics/views/orders_view", null);
        assertEquals(200, get.statusCode);
        JsonNode getMetadata = MAPPER.readTree(get.body).path("metadata");
        assertEquals(2, getMetadata.path("versions").size());

        HttpResponse deleteView = request("DELETE", "/v1/namespaces/analytics/views/orders_view", null);
        assertEquals(204, deleteView.statusCode);
        assertEquals(404, request("GET", "/v1/namespaces/analytics/views/orders_view", null).statusCode);
    }

    @Test
    void viewPaginationReturnsNextPageTokenAndContinuation() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");

        String createA = """
                {"name":"a","view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[]}}
                """;
        String createB = """
                {"name":"b","view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[]}}
                """;
        String createC = """
                {"name":"c","view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[]}}
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views", createA).statusCode);
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views", createB).statusCode);
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views", createC).statusCode);

        HttpResponse page1 = request("GET", "/v1/namespaces/analytics/views?page-size=2", null);
        assertEquals(200, page1.statusCode);
        JsonNode page1Json = MAPPER.readTree(page1.body);
        assertEquals(2, page1Json.path("identifiers").size());
        assertEquals("b", page1Json.path("next-page-token").asText());

        HttpResponse page2 = request("GET", "/v1/namespaces/analytics/views?page-size=2&page-token=b", null);
        assertEquals(200, page2.statusCode);
        JsonNode page2Json = MAPPER.readTree(page2.body);
        assertEquals(1, page2Json.path("identifiers").size());
        assertEquals("c", page2Json.path("identifiers").get(0).path("name").asText());
        assertTrue(page2Json.path("next-page-token").isMissingNode());
    }

    @Test
    void viewRemoveVersionsCommitPathsReturnExpectedStatus() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");

        String createViewBody = """
                {
                  "name":"orders_view",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]}
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views", createViewBody).statusCode);

        String promoteV2 = """
                {
                  "updates":[
                    {"action":"add-view-version","view-version":{"version-id":2,"schema-id":0,"timestamp-ms":1700000001000}},
                    {"action":"set-current-view-version","version-id":2}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views/orders_view", promoteV2).statusCode);

        String removeOld = """
                {"updates":[{"action":"remove-view-versions","version-ids":[1]}]}
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views/orders_view", removeOld).statusCode);

        String removeCurrent = """
                {"updates":[{"action":"remove-view-versions","version-ids":[2]}]}
                """;
        HttpResponse conflict = request("POST", "/v1/namespaces/analytics/views/orders_view", removeCurrent);
        assertEquals(409, conflict.statusCode);

        String removeUnknown = """
                {"updates":[{"action":"remove-view-versions","version-ids":[999]}]}
                """;
        HttpResponse badUnknown = request("POST", "/v1/namespaces/analytics/views/orders_view", removeUnknown);
        assertEquals(400, badUnknown.statusCode);

        String malformed = """
                {"updates":[{"action":"remove-view-versions","version-ids":"bad"}]}
                """;
        HttpResponse badMalformed = request("POST", "/v1/namespaces/analytics/views/orders_view", malformed);
        assertEquals(400, badMalformed.statusCode);
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

        HttpResponse malformedAddEncryptionKey = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"add-encryption-key\",\"encryption-key\":{\"wrapped-key\":\"abc\"}}]}");
        assertEquals(400, malformedAddEncryptionKey.statusCode);

        HttpResponse malformedRemoveEncryptionKey = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"remove-encryption-key\",\"key-ids\":\"bad\"}]}");
        assertEquals(400, malformedRemoveEncryptionKey.statusCode);
    }

    @Test
    void reportMetricsValidatesPayloadAndPersistsReports() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        String commitReport = """
                {
                  "report-type":"commit-report",
                  "report":{"table-name":"orders","snapshot-id":123}
                }
                """;
        HttpResponse ok = request("POST", "/v1/namespaces/analytics/tables/orders/metrics", commitReport);
        assertEquals(204, ok.statusCode);
        List<MetricsStore.MetricRecord> records = metricsStore.listTableMetrics("analytics", "orders");
        assertEquals(1, records.size());
        assertEquals("commit-report", records.get(0).reportType());

        HttpResponse missingTable = request("POST", "/v1/namespaces/analytics/tables/missing/metrics", commitReport);
        assertEquals(404, missingTable.statusCode);
        assertEquals("NoSuchEntityException", MAPPER.readTree(missingTable.body).path("error").path("type").asText());

        HttpResponse badJson = request("POST", "/v1/namespaces/analytics/tables/orders/metrics", "{\"report-type\"");
        assertEquals(400, badJson.statusCode);

        HttpResponse badType = request("POST", "/v1/namespaces/analytics/tables/orders/metrics", "{\"report-type\":\"x\",\"report\":{}}");
        assertEquals(400, badType.statusCode);

        HttpResponse missingReport = request("POST", "/v1/namespaces/analytics/tables/orders/metrics", "{\"report-type\":\"scan-report\"}");
        assertEquals(400, missingReport.statusCode);
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

        HttpResponse nonStringRequirementType = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":123}],\"updates\":[]}");
        assertEquals(400, nonStringRequirementType.statusCode);

        HttpResponse assertCreateWithUnexpectedField = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-create\",\"unexpected\":true}],\"updates\":[]}");
        assertEquals(400, assertCreateWithUnexpectedField.statusCode);

        HttpResponse malformedAssertRef = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"snapshot-id\":null}],\"updates\":[]}");
        assertEquals(400, malformedAssertRef.statusCode);

        HttpResponse malformedAssertRefExtraField = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":null,\"unexpected\":\"x\"}],\"updates\":[]}");
        assertEquals(400, malformedAssertRefExtraField.statusCode);

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

        HttpResponse removeMainRefConflict = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"remove-snapshot-ref\",\"ref-name\":\"main\"}]}");
        assertEquals(409, removeMainRefConflict.statusCode);

        HttpResponse malformedRemoveSnapshots = request(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"updates\":[{\"action\":\"remove-snapshots\",\"snapshot-ids\":\"nope\"}]}");
        assertEquals(400, malformedRemoveSnapshots.statusCode);
    }

    @Test
    void requirementFailureErrorEnvelopeMatrixIncludesStatusTypeCodeAndMessageClass() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");
        String tableBody = "{\"name\":\"orders\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}}";
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables", tableBody).statusCode);

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-unknown\",\"value\":1}],\"updates\":[]}",
                400,
                "BadRequestException",
                "Unsupported requirement type");

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":123}],\"updates\":[]}",
                400,
                "BadRequestException",
                "Requirement type is missing");

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-create\"}],\"updates\":[]}",
                409,
                "CommitFailedException",
                "assert-create failed");

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-current-schema-id\",\"current-schema-id\":\"0\"}],\"updates\":[]}",
                400,
                "BadRequestException",
                "requires integer current-schema-id");

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-current-schema-id\",\"current-schema-id\":1}],\"updates\":[]}",
                409,
                "CommitFailedException",
                "assert-current-schema-id failed");

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"snapshot-id\":null}],\"updates\":[]}",
                400,
                "BadRequestException",
                "requires non-empty ref");

        String firstSnapshotCommit = """
                {
                  "requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
                  "updates":[
                    {"action":"add-snapshot","snapshot":{"sequence-number":1,"snapshot-id":101,"timestamp-ms":1700000000000,"schema-id":0}},
                    {"action":"set-snapshot-ref","ref-name":"main","snapshot-id":101,"type":"branch"}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", firstSnapshotCommit).statusCode);

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/tables/orders",
                "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":null}],\"updates\":[]}",
                409,
                "CommitFailedException",
                "assert-ref-snapshot-id failed");

        String viewBody = """
                {"name":"orders_v","view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},"schema":{"type":"struct","schema-id":0,"fields":[]}}
                """;
        HttpResponse createView = request("POST", "/v1/namespaces/analytics/views", viewBody);
        assertEquals(200, createView.statusCode);
        String viewUuid = MAPPER.readTree(createView.body).path("metadata").path("view-uuid").asText();

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/views/orders_v",
                "{\"requirements\":[{\"type\":\"assert-view-uuid\",\"uuid\":\"%s\",\"extra\":\"x\"}],\"updates\":[]}".formatted(viewUuid),
                400,
                "BadRequestException",
                "has unsupported field");

        assertErrorEnvelope(
                "POST",
                "/v1/namespaces/analytics/views/orders_v",
                "{\"requirements\":[{\"type\":\"assert-view-uuid\",\"uuid\":\"11111111-1111-1111-1111-111111111111\"}],\"updates\":[]}",
                409,
                "CommitFailedException",
                "assert-view-uuid failed");
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
    void namespacePaginationWithParentFilterReturnsNextPageTokenAndContinuation() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\",\"a\"],\"properties\":{}}").statusCode);
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\",\"b\"],\"properties\":{}}").statusCode);
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\",\"c\"],\"properties\":{}}").statusCode);

        HttpResponse page1 = request("GET", "/v1/namespaces?parent=analytics&page-size=2", null);
        assertEquals(200, page1.statusCode);
        JsonNode page1Json = MAPPER.readTree(page1.body);
        assertEquals(2, page1Json.path("namespaces").size());
        assertEquals("analytics.b", page1Json.path("next-page-token").asText());

        HttpResponse page2 = request("GET", "/v1/namespaces?parent=analytics&page-size=2&page-token=analytics.b", null);
        assertEquals(200, page2.statusCode);
        JsonNode page2Json = MAPPER.readTree(page2.body);
        assertEquals(1, page2Json.path("namespaces").size());
        assertTrue(page2Json.path("next-page-token").isMissingNode());
    }

    @Test
    void paginationRejectsInvalidPageParameters() throws Exception {
        request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}");

        HttpResponse badSize = request("GET", "/v1/namespaces?page-size=0", null);
        assertEquals(400, badSize.statusCode);

        HttpResponse badToken = request("GET", "/v1/namespaces?page-token=missing", null);
        assertEquals(400, badToken.statusCode);
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
    void deleteNamespaceConflictsWhenViewsExist() throws Exception {
        assertEquals(200, request("POST", "/v1/namespaces", "{\"namespace\":[\"analytics\"],\"properties\":{}}").statusCode);
        String createViewBody = """
                {
                  "name":"v1",
                  "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0},
                  "schema":{"type":"struct","schema-id":0,"fields":[]}
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/views", createViewBody).statusCode);

        HttpResponse namespaceDeleteConflict = request("DELETE", "/v1/namespaces/analytics", null);
        assertEquals(409, namespaceDeleteConflict.statusCode);

        assertEquals(204, request("DELETE", "/v1/namespaces/analytics/views/v1", null).statusCode);
        assertEquals(204, request("DELETE", "/v1/namespaces/analytics", null).statusCode);
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

        String addDevRefCommit = """
                {
                  "updates":[
                    {"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":1001,"type":"branch"}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", addDevRefCommit).statusCode);

        String removeDevRefCommit = """
                {
                  "updates":[
                    {"action":"remove-snapshot-ref","ref-name":"dev"}
                  ]
                }
                """;
        assertEquals(200, request("POST", "/v1/namespaces/analytics/tables/orders", removeDevRefCommit).statusCode);

        String removeSnapshotsCommit = """
                {
                  "updates":[
                    {"action":"remove-snapshots","snapshot-ids":[1001]}
                  ]
                }
                """;
        HttpResponse removeMainSnapshotConflict = request("POST", "/v1/namespaces/analytics/tables/orders", removeSnapshotsCommit);
        assertEquals(409, removeMainSnapshotConflict.statusCode);

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
        assertTrue(metadata.path("refs").has("main"));
        assertTrue(!metadata.path("refs").has("dev"));
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

    private void assertErrorEnvelope(String method, String path, String body, int expectedCode, String expectedType, String messageContains) throws Exception {
        HttpResponse response = request(method, path, body);
        assertEquals(expectedCode, response.statusCode);
        JsonNode error = MAPPER.readTree(response.body).path("error");
        assertEquals(expectedCode, error.path("code").asInt());
        assertEquals(expectedType, error.path("type").asText());
        assertTrue(error.path("message").asText().contains(messageContains));
    }

    private record HttpResponse(int statusCode, String body) {}
}
