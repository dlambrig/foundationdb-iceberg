import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IcebergRestMockServer {
    private static final int PORT = 8181;
    private static final Pattern NAMESPACE_PATTERN = Pattern.compile("\\\"namespace\\\"\\s*:\\s*\\[\\s*\\\"([^\\\"]+)\\\"\\s*\\]");
    private static final String CONFIG_RESPONSE = "{\"defaults\":{},\"overrides\":{}}";
    private static final String TABLE_PATH_PREFIX = "/v1/namespaces/";
    private static final String TABLES_SEGMENT = "/tables";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Path DEFAULT_LOCAL_ROOT = Paths.get(System.getProperty("java.io.tmpdir"));
    private static final String DEFAULT_WAREHOUSE_LOCATION = "local:///iceberg_warehouse";

    private static NamespaceStore namespaceStore;
    private static TableStore tableStore;

    public static void main(String[] args) throws IOException {
        boolean useFdb = isFdbEnabled(args);
        namespaceStore = buildNamespaceStore(useFdb);
        tableStore = buildTableStore(useFdb);

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/v1/config", new ConfigHandler());
        server.createContext("/v1/namespaces", new NamespacesHandler());
        server.createContext("/", new NotFoundHandler());
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();

        System.out.println("Iceberg REST mock server listening on http://localhost:" + PORT);
        System.out.println("Serving GET /v1/config");
        System.out.println("Serving GET /v1/namespaces");
        System.out.println("Serving POST /v1/namespaces");
        System.out.println("Serving GET /v1/namespaces/{ns}/tables/{table}");
        System.out.println("Serving POST /v1/namespaces/{ns}/tables");
    }

    private static boolean isFdbEnabled(String[] args) {
        return Boolean.getBoolean("fdb") || Arrays.asList(args).contains("--fdb");
    }

    private static NamespaceStore buildNamespaceStore(boolean useFdb) {
        if (!useFdb) {
            System.out.println("Storage mode: MEMORY");
            return new InMemoryNamespaceStore(List.of("sales"));
        }

        System.out.println("Storage mode: FDB");
        try {
            return new FdbNamespaceStore();
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to initialize FoundationDB storage mode", e);
        }
    }

    private static TableStore buildTableStore(boolean useFdb) {
        if (!useFdb) {
            return new InMemoryTableStore();
        }
        return new FdbTableStore();
    }

    private interface NamespaceStore {
        List<String> listNamespaces();

        void createNamespace(String namespace);
    }

    private interface TableStore {
        String getTableResponse(String namespace, String table);

        void putTableResponse(String namespace, String table, String responseJson);

        List<String> listTables(String namespace);
    }

    private static class InMemoryNamespaceStore implements NamespaceStore {
        private final Set<String> namespaces = new LinkedHashSet<>();

        private InMemoryNamespaceStore(List<String> initialNamespaces) {
            namespaces.addAll(initialNamespaces);
        }

        @Override
        public synchronized List<String> listNamespaces() {
            return new ArrayList<>(namespaces);
        }

        @Override
        public synchronized void createNamespace(String namespace) {
            namespaces.add(namespace);
        }
    }

    private static class FdbNamespaceStore implements NamespaceStore {
        private static final Tuple PREFIX_TUPLE = Tuple.from("iceberg-rest-mock", "namespace");
        private final FDBDatabase database;
        private final byte[] prefixBytes;

        private FdbNamespaceStore() {
            this.database = FDBDatabaseFactory.instance().getDatabase();
            this.prefixBytes = PREFIX_TUPLE.pack();
        }

        @Override
        public List<String> listNamespaces() {
            return database.run(context -> {
                List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefixBytes)).asList().join();
                List<String> namespaces = new ArrayList<>();
                for (KeyValue row : rows) {
                    Tuple keyTuple = Tuple.fromBytes(row.getKey());
                    Object value = keyTuple.get(keyTuple.size() - 1);
                    namespaces.add(String.valueOf(value));
                }
                Collections.sort(namespaces);
                return namespaces;
            });
        }

        @Override
        public void createNamespace(String namespace) {
            database.run(context -> {
                byte[] key = Tuple.from("iceberg-rest-mock", "namespace", namespace).pack();
                context.ensureActive().set(key, new byte[] {1});
                return null;
            });
        }
    }

    private static class InMemoryTableStore implements TableStore {
        private final Map<String, String> tables = new HashMap<>();

        @Override
        public synchronized String getTableResponse(String namespace, String table) {
            return tables.get(key(namespace, table));
        }

        @Override
        public synchronized void putTableResponse(String namespace, String table, String responseJson) {
            tables.put(key(namespace, table), responseJson);
        }

        @Override
        public synchronized List<String> listTables(String namespace) {
            List<String> results = new ArrayList<>();
            String prefix = namespace + ".";
            for (String key : tables.keySet()) {
                if (key.startsWith(prefix)) {
                    results.add(key.substring(prefix.length()));
                }
            }
            Collections.sort(results);
            return results;
        }

        private static String key(String namespace, String table) {
            return namespace + "." + table;
        }
    }

    private static class FdbTableStore implements TableStore {
        private final FDBDatabase database;

        private FdbTableStore() {
            this.database = FDBDatabaseFactory.instance().getDatabase();
        }

        @Override
        public String getTableResponse(String namespace, String table) {
            return database.run(context -> {
                byte[] key = Tuple.from("iceberg-rest-mock", "table", namespace, table).pack();
                byte[] value = context.ensureActive().get(key).join();
                if (value == null) {
                    return null;
                }
                return new String(value, StandardCharsets.UTF_8);
            });
        }

        @Override
        public void putTableResponse(String namespace, String table, String responseJson) {
            database.run(context -> {
                byte[] key = Tuple.from("iceberg-rest-mock", "table", namespace, table).pack();
                context.ensureActive().set(key, responseJson.getBytes(StandardCharsets.UTF_8));
                return null;
            });
        }

        @Override
        public List<String> listTables(String namespace) {
            return database.run(context -> {
                byte[] prefix = Tuple.from("iceberg-rest-mock", "table", namespace).pack();
                List<KeyValue> rows = context.ensureActive().getRange(Range.startsWith(prefix)).asList().join();
                List<String> tables = new ArrayList<>();
                for (KeyValue row : rows) {
                    Tuple keyTuple = Tuple.fromBytes(row.getKey());
                    Object value = keyTuple.get(keyTuple.size() - 1);
                    tables.add(String.valueOf(value));
                }
                Collections.sort(tables);
                return tables;
            });
        }
    }

    private static class ConfigHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            System.out.println("Received " + method + " " + path);

            if (!"GET".equals(method)) {
                sendIcebergError(exchange, 405, "Method Not Allowed", method, path);
                return;
            }

            sendJson(exchange, 200, CONFIG_RESPONSE, method, path);
        }
    }

    private static class NamespacesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            System.out.println("Received " + method + " " + path);

            try {
                if (path.startsWith(TABLE_PATH_PREFIX) && path.contains(TABLES_SEGMENT)) {
                    handleTableRoutes(exchange, method, path);
                    return;
                }

                if ("GET".equals(method)) {
                    handleGetNamespace(exchange, method, path);
                    return;
                }

                if ("POST".equals(method)) {
                    if (!"/v1/namespaces".equals(path) && !"/v1/namespaces/".equals(path)) {
                        sendIcebergError(exchange, 404, "Not Found", method, path);
                        return;
                    }
                    handleCreateNamespace(exchange, method, path);
                    return;
                }
            } catch (RuntimeException e) {
                sendIcebergError(exchange, 500, "Internal Server Error", method, path);
                throw e;
            }

            sendIcebergError(exchange, 405, "Method Not Allowed", method, path);
        }

        private void handleTableRoutes(HttpExchange exchange, String method, String path) throws IOException {
            String suffix = path.substring(TABLE_PATH_PREFIX.length());
            String[] parts = suffix.split("/");
            if (parts.length < 2 || !"tables".equals(parts[1])) {
                sendIcebergError(exchange, 404, "Not Found", method, path);
                return;
            }

            String namespace = parts[0];
            if (!namespaceStore.listNamespaces().contains(namespace)) {
                sendIcebergError(exchange, 404, "Namespace not found: " + namespace, method, path);
                return;
            }

            if ("POST".equals(method) && parts.length == 2) {
                handleCreateTable(exchange, method, path, namespace);
                return;
            }

            if ("GET".equals(method) && parts.length == 2) {
                handleListTables(exchange, method, path, namespace);
                return;
            }

            if ("GET".equals(method) && parts.length == 3) {
                handleGetTable(exchange, method, path, namespace, parts[2]);
                return;
            }

            if ("POST".equals(method) && parts.length == 3) {
                handleCommitTable(exchange, method, path, namespace, parts[2]);
                return;
            }

            if ("POST".equals(method) && parts.length == 4 && "metrics".equals(parts[3])) {
                sendNoContent(exchange, method, path);
                return;
            }

            sendIcebergError(exchange, 404, "Not Found", method, path);
        }

        private void handleGetNamespace(HttpExchange exchange, String method, String path) throws IOException {
            if ("/v1/namespaces".equals(path) || "/v1/namespaces/".equals(path)) {
                sendJson(exchange, 200, listNamespacesJson(), method, path);
                return;
            }

            if (!path.startsWith("/v1/namespaces/")) {
                sendIcebergError(exchange, 404, "Not Found", method, path);
                return;
            }

            String suffix = path.substring("/v1/namespaces/".length());
            if (suffix.isEmpty() || suffix.contains("/")) {
                sendIcebergError(exchange, 404, "Not Found", method, path);
                return;
            }

            if (!namespaceStore.listNamespaces().contains(suffix)) {
                sendIcebergError(exchange, 404, "Namespace not found: " + suffix, method, path);
                return;
            }

            sendJson(exchange, 200, "{\"namespace\":[\"" + suffix + "\"],\"properties\":{}}", method, path);
        }

        private void handleGetTable(HttpExchange exchange, String method, String path, String namespace, String table) throws IOException {
            String responseJson = tableStore.getTableResponse(namespace, table);
            if (responseJson == null) {
                sendIcebergError(exchange, 404, "Table not found: " + namespace + "." + table, method, path);
                return;
            }
            String normalizedResponseJson = normalizeMetadataLocation(responseJson);
            persistMetadataFile(normalizedResponseJson);
            tableStore.putTableResponse(namespace, table, normalizedResponseJson);
            sendJson(exchange, 200, normalizedResponseJson, method, path);
        }

        private void handleListTables(HttpExchange exchange, String method, String path, String namespace) throws IOException {
            List<String> tables = tableStore.listTables(namespace);
            StringBuilder body = new StringBuilder();
            body.append("{\"identifiers\":[");
            for (int i = 0; i < tables.size(); i++) {
                if (i > 0) {
                    body.append(",");
                }
                body.append("{\"namespace\":[\"")
                        .append(namespace)
                        .append("\"],\"name\":\"")
                        .append(escapeJson(tables.get(i)))
                        .append("\"}");
            }
            body.append("]}");
            sendJson(exchange, 200, body.toString(), method, path);
        }

        private void handleCreateTable(HttpExchange exchange, String method, String path, String namespace) throws IOException {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Request body: " + requestBody);

            String loadTableResponseJson;
            try {
                loadTableResponseJson = buildLoadTableResponseJson(namespace, requestBody);
            } catch (IllegalArgumentException e) {
                sendIcebergError(exchange, 400, e.getMessage(), method, path);
                return;
            }

            JsonNode root = OBJECT_MAPPER.readTree(requestBody);
            String tableName = root.path("name").asText();
            String normalizedResponseJson = normalizeMetadataLocation(loadTableResponseJson);
            tableStore.putTableResponse(namespace, tableName, normalizedResponseJson);
            persistMetadataFile(normalizedResponseJson);
            sendJson(exchange, 200, normalizedResponseJson, method, path);
        }

        private void handleCommitTable(HttpExchange exchange, String method, String path, String namespace, String table) throws IOException {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Request body: " + requestBody);
            String existingResponseJson = tableStore.getTableResponse(namespace, table);
            if (existingResponseJson == null) {
                sendIcebergError(exchange, 404, "Table not found: " + namespace + "." + table, method, path);
                return;
            }

            String updatedResponseJson;
            try {
                updatedResponseJson = applyCommitToTableResponseJson(existingResponseJson, requestBody);
            } catch (IllegalArgumentException e) {
                sendIcebergError(exchange, 400, e.getMessage(), method, path);
                return;
            }

            String normalizedResponseJson = normalizeMetadataLocation(updatedResponseJson);
            tableStore.putTableResponse(namespace, table, normalizedResponseJson);
            persistMetadataFile(normalizedResponseJson);
            sendJson(exchange, 200, normalizedResponseJson, method, path);
        }

        private void handleCreateNamespace(HttpExchange exchange, String method, String path) throws IOException {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Request body: " + requestBody);
            Matcher matcher = NAMESPACE_PATTERN.matcher(requestBody);

            if (!matcher.find()) {
                sendIcebergError(exchange, 400, "Invalid request body", method, path);
                return;
            }

            String namespaceValue = matcher.group(1).trim();
            if (namespaceValue.isEmpty()) {
                sendIcebergError(exchange, 400, "Invalid namespace", method, path);
                return;
            }

            namespaceStore.createNamespace(namespaceValue);
            sendJson(exchange, 200, "{\"namespace\":[\"" + namespaceValue + "\"]}", method, path);
        }

        private String buildLoadTableResponseJson(String namespace, String createTableRequestBody) {
            JsonNode request;
            try {
                request = OBJECT_MAPPER.readTree(createTableRequestBody);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid JSON request body");
            }

            String tableName = request.path("name").asText();
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("Missing table name");
            }

            JsonNode schemaNode = request.get("schema");
            if (schemaNode == null || schemaNode.isMissingNode() || !schemaNode.isObject()) {
                throw new IllegalArgumentException("Missing or invalid schema");
            }

            ObjectNode schema = schemaNode.deepCopy();
            if (!schema.has("schema-id")) {
                schema.put("schema-id", 0);
            }

            long lastColumnId = 0;
            JsonNode fieldsNode = schema.get("fields");
            if (fieldsNode != null && fieldsNode.isArray()) {
                for (JsonNode field : fieldsNode) {
                    long id = field.path("id").asLong(0);
                    if (id > lastColumnId) {
                        lastColumnId = id;
                    }
                }
            }

            String tableUuid = UUID.randomUUID().toString();
            String tableLocation = DEFAULT_WAREHOUSE_LOCATION + "/" + namespace + "/" + tableName;
            String metadataLocation = DEFAULT_WAREHOUSE_LOCATION + "/_rest_metadata/" + tableUuid + "/00000-" + tableUuid + ".metadata.json";

            ObjectNode metadata = OBJECT_MAPPER.createObjectNode();
            metadata.put("format-version", 2);
            metadata.put("table-uuid", tableUuid);
            metadata.put("location", tableLocation);
            metadata.put("last-sequence-number", 0);
            metadata.put("last-updated-ms", System.currentTimeMillis());
            metadata.put("last-column-id", lastColumnId);
            metadata.put("current-schema-id", schema.path("schema-id").asInt(0));

            ArrayNode schemas = metadata.putArray("schemas");
            schemas.add(schema);

            metadata.put("default-spec-id", 0);
            ArrayNode partitionSpecs = metadata.putArray("partition-specs");
            ObjectNode spec = OBJECT_MAPPER.createObjectNode();
            spec.put("spec-id", 0);
            spec.putArray("fields");
            partitionSpecs.add(spec);

            metadata.put("last-partition-id", 999);
            metadata.put("default-sort-order-id", 0);
            ArrayNode sortOrders = metadata.putArray("sort-orders");
            ObjectNode sortOrder = OBJECT_MAPPER.createObjectNode();
            sortOrder.put("order-id", 0);
            sortOrder.putArray("fields");
            sortOrders.add(sortOrder);

            metadata.putObject("properties");
            metadata.put("current-snapshot-id", -1);
            metadata.putObject("refs");
            metadata.putArray("snapshots");
            metadata.putArray("statistics");
            metadata.putArray("partition-statistics");
            metadata.putArray("snapshot-log");
            metadata.putArray("metadata-log");

            ObjectNode response = OBJECT_MAPPER.createObjectNode();
            response.put("metadata-location", metadataLocation);
            response.set("metadata", metadata);
            response.putObject("config");

            try {
                return OBJECT_MAPPER.writeValueAsString(response);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to build table response");
            }
        }

        private String applyCommitToTableResponseJson(String existingResponseJson, String commitRequestBody) {
            JsonNode existingRoot;
            JsonNode commitRoot;
            try {
                existingRoot = OBJECT_MAPPER.readTree(existingResponseJson);
                commitRoot = OBJECT_MAPPER.readTree(commitRequestBody);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid JSON payload");
            }

            ObjectNode updatedRoot = existingRoot.deepCopy();
            JsonNode metadataNode = updatedRoot.get("metadata");
            if (metadataNode == null || !metadataNode.isObject()) {
                throw new IllegalArgumentException("Stored table metadata is invalid");
            }
            ObjectNode metadata = (ObjectNode) metadataNode;

            ArrayNode snapshots = ensureArray(metadata, "snapshots");
            ObjectNode refs = ensureObject(metadata, "refs");
            ArrayNode statistics = ensureArray(metadata, "statistics");
            ArrayNode snapshotLog = ensureArray(metadata, "snapshot-log");
            ArrayNode schemas = ensureArray(metadata, "schemas");

            long maxSequence = metadata.path("last-sequence-number").asLong(0);
            JsonNode updatesNode = commitRoot.get("updates");
            if (updatesNode != null && updatesNode.isArray()) {
                for (JsonNode updateNode : updatesNode) {
                    String action = updateNode.path("action").asText("");
                    if ("add-snapshot".equals(action)) {
                        JsonNode snapshot = updateNode.get("snapshot");
                        if (snapshot != null && snapshot.isObject()) {
                            snapshots.add(snapshot.deepCopy());
                            long seq = snapshot.path("sequence-number").asLong(0);
                            if (seq > maxSequence) {
                                maxSequence = seq;
                            }
                            long snapshotId = snapshot.path("snapshot-id").asLong(-1);
                            long timestamp = snapshot.path("timestamp-ms").asLong(System.currentTimeMillis());
                            if (snapshotId >= 0) {
                                ObjectNode logEntry = OBJECT_MAPPER.createObjectNode();
                                logEntry.put("timestamp-ms", timestamp);
                                logEntry.put("snapshot-id", snapshotId);
                                snapshotLog.add(logEntry);
                            }
                        }
                    } else if ("set-snapshot-ref".equals(action)) {
                        String refName = updateNode.path("ref-name").asText("");
                        long snapshotId = updateNode.path("snapshot-id").asLong(-1);
                        if (!refName.isEmpty() && snapshotId >= 0) {
                            ObjectNode ref = OBJECT_MAPPER.createObjectNode();
                            ref.put("snapshot-id", snapshotId);
                            String refType = updateNode.path("type").asText("");
                            if (!refType.isEmpty()) {
                                ref.put("type", refType);
                            }
                            refs.set(refName, ref);
                            if ("main".equals(refName)) {
                                metadata.put("current-snapshot-id", snapshotId);
                            }
                        }
                    } else if ("set-statistics".equals(action)) {
                        JsonNode stats = updateNode.get("statistics");
                        if (stats != null && stats.isObject()) {
                            statistics.add(stats.deepCopy());
                        }
                    } else if ("add-schema".equals(action)) {
                        JsonNode schema = updateNode.get("schema");
                        if (schema != null && schema.isObject()) {
                            schemas.add(schema.deepCopy());
                        }
                        long lastColumnId = updateNode.path("last-column-id").asLong(metadata.path("last-column-id").asLong(0));
                        metadata.put("last-column-id", lastColumnId);
                    } else if ("set-current-schema".equals(action)) {
                        int schemaId = updateNode.path("schema-id").asInt(Integer.MIN_VALUE);
                        if (schemaId == -1) {
                            int latestSchemaId = metadata.path("current-schema-id").asInt(0);
                            for (JsonNode schemaNode : schemas) {
                                int candidate = schemaNode.path("schema-id").asInt(latestSchemaId);
                                if (candidate > latestSchemaId) {
                                    latestSchemaId = candidate;
                                }
                            }
                            metadata.put("current-schema-id", latestSchemaId);
                        } else if (schemaId != Integer.MIN_VALUE) {
                            metadata.put("current-schema-id", schemaId);
                        }
                    }
                }
            }

            metadata.put("last-sequence-number", maxSequence);
            metadata.put("last-updated-ms", System.currentTimeMillis());

            try {
                return OBJECT_MAPPER.writeValueAsString(updatedRoot);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to serialize updated table metadata");
            }
        }

        private ArrayNode ensureArray(ObjectNode parent, String fieldName) {
            JsonNode node = parent.get(fieldName);
            if (node == null || !node.isArray()) {
                return parent.putArray(fieldName);
            }
            return (ArrayNode) node;
        }

        private ObjectNode ensureObject(ObjectNode parent, String fieldName) {
            JsonNode node = parent.get(fieldName);
            if (node == null || !node.isObject()) {
                return parent.putObject(fieldName);
            }
            return (ObjectNode) node;
        }


        private void persistMetadataFile(String loadTableResponseJson) {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(loadTableResponseJson);
                String metadataLocation = root.path("metadata-location").asText(null);
                JsonNode metadata = root.get("metadata");
                if (metadataLocation == null || metadata == null || metadata.isMissingNode()) {
                    return;
                }

                Path metadataPath = resolveWritablePath(metadataLocation);
                if (metadataPath == null) {
                    return;
                }
                Path parent = metadataPath.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.writeString(metadataPath, OBJECT_MAPPER.writeValueAsString(metadata), StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException("Failed to persist metadata file", e);
            }
        }

        private Path resolveWritablePath(String location) {
            URI uri = URI.create(location);
            String scheme = uri.getScheme();
            if ("file".equalsIgnoreCase(scheme)) {
                return Paths.get(uri);
            }
            if ("local".equalsIgnoreCase(scheme)) {
                String relativePath = uri.getPath();
                while (relativePath.startsWith("/")) {
                    relativePath = relativePath.substring(1);
                }
                return DEFAULT_LOCAL_ROOT.resolve(relativePath).normalize();
            }
            return null;
        }

        private String normalizeMetadataLocation(String loadTableResponseJson) {
            return loadTableResponseJson;
        }

        private static String listNamespacesJson() {
            List<String> namespaces = namespaceStore.listNamespaces();
            StringBuilder builder = new StringBuilder();
            builder.append("{\"namespaces\":[");
            for (int i = 0; i < namespaces.size(); i++) {
                if (i > 0) {
                    builder.append(",");
                }
                builder.append("[\"").append(namespaces.get(i)).append("\"]");
            }
            builder.append("]}");
            return builder.toString();
        }
    }

    private static class NotFoundHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            System.out.println("Received " + method + " " + path);
            sendIcebergError(exchange, 404, "Not Found", method, path);
        }
    }

    private static void sendIcebergError(HttpExchange exchange, int statusCode, String message, String method, String path) throws IOException {
        String escapedMessage = escapeJson(message);
        String type;
        if (statusCode >= 500) {
            type = "ServerErrorException";
        } else if (statusCode == 404) {
            type = "NoSuchEntityException";
        } else if (statusCode == 409) {
            type = "CommitFailedException";
        } else {
            type = "BadRequestException";
        }
        String body = "{\"error\":{\"message\":\"" + escapedMessage + "\",\"type\":\"" + type + "\",\"code\":" + statusCode + "}}";
        sendJson(exchange, statusCode, body, method, path);
    }

    private static void sendJson(HttpExchange exchange, int statusCode, String body, String method, String path) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, response.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
        }
        System.out.println("Replied " + statusCode + " to " + method + " " + path + " with: " + body);
    }

    private static void sendNoContent(HttpExchange exchange, String method, String path) throws IOException {
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
        System.out.println("Replied 204 to " + method + " " + path);
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
