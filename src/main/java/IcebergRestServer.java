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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.Executors;

public class IcebergRestServer {
    private static final int PORT = 8181;
    private static final String CONFIG_RESPONSE = "{\"defaults\":{},\"overrides\":{}}";
    private static final String TABLE_PATH_PREFIX = "/v1/namespaces/";
    private static final String TABLES_SEGMENT = "/tables";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Path DEFAULT_LOCAL_ROOT = Paths.get(System.getProperty("java.io.tmpdir"));
    private static final String DEFAULT_WAREHOUSE_LOCATION = "local:///iceberg_warehouse";
    private static final Pattern METADATA_FILE_PATTERN = Pattern.compile("^(\\d+)-(.+)\\.metadata\\.json$");

    private static NamespaceStore namespaceStore;
    private static TableStore tableStore;
    private static final Map<String, Map<String, String>> namespaceProperties = new HashMap<>();

    public static void main(String[] args) throws IOException {
        boolean useFdb = isFdbEnabled(args);
        startServer(PORT, buildNamespaceStore(useFdb), buildTableStore(useFdb));
        System.out.println("Iceberg REST server listening on http://localhost:" + PORT);
        System.out.println("Serving GET /v1/config");
        System.out.println("Serving GET /v1/namespaces");
        System.out.println("Serving POST /v1/namespaces");
        System.out.println("Serving GET /v1/namespaces/{ns}/tables/{table}");
        System.out.println("Serving POST /v1/namespaces/{ns}/tables");
    }

    static HttpServer startServer(int port, NamespaceStore nsStore, TableStore tblStore) throws IOException {
        namespaceStore = nsStore;
        tableStore = tblStore;
        synchronized (namespaceProperties) {
            namespaceProperties.clear();
            for (String namespace : namespaceStore.listNamespaces()) {
                namespaceProperties.put(namespace, new LinkedHashMap<>());
            }
        }
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v1/config", new ConfigHandler());
        server.createContext("/v1/namespaces", new NamespacesHandler());
        server.createContext("/v1/tables", new NamespacesHandler());
        server.createContext("/", new NotFoundHandler());
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        return server;
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

    static String buildLoadTableResponseJson(String namespace, String createTableRequestBody) {
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

    static String applyCommitToTableResponseJson(String existingResponseJson, String commitRequestBody) {
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
        String currentMetadataLocation = updatedRoot.path("metadata-location").asText("");
        if (currentMetadataLocation.isEmpty()) {
            throw new IllegalArgumentException("Stored table metadata-location is invalid");
        }

        validateCommitRequirements(commitRoot, metadata);

        ArrayNode snapshots = ensureArray(metadata, "snapshots");
        ObjectNode refs = ensureObject(metadata, "refs");
        ArrayNode statistics = ensureArray(metadata, "statistics");
        ArrayNode snapshotLog = ensureArray(metadata, "snapshot-log");
        ArrayNode metadataLog = ensureArray(metadata, "metadata-log");
        ArrayNode schemas = ensureArray(metadata, "schemas");

        long maxSequence = metadata.path("last-sequence-number").asLong(0);
        JsonNode updatesNode = commitRoot.get("updates");
        if (updatesNode == null || !updatesNode.isArray()) {
            throw new IllegalArgumentException("Missing or invalid updates");
        }
        for (JsonNode updateNode : updatesNode) {
            String action = updateNode.path("action").asText("");
            if (action.isEmpty()) {
                throw new IllegalArgumentException("Missing update action");
            }
            if ("add-snapshot".equals(action)) {
                JsonNode snapshot = updateNode.get("snapshot");
                if (snapshot == null || !snapshot.isObject()) {
                    throw new IllegalArgumentException("add-snapshot requires snapshot object");
                }
                if (!snapshot.has("snapshot-id") || !snapshot.has("sequence-number")) {
                    throw new IllegalArgumentException("add-snapshot requires snapshot-id and sequence-number");
                }
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
            } else if ("set-snapshot-ref".equals(action)) {
                String refName = updateNode.path("ref-name").asText("");
                long snapshotId = updateNode.path("snapshot-id").asLong(Long.MIN_VALUE);
                if (refName.isEmpty() || snapshotId == Long.MIN_VALUE) {
                    throw new IllegalArgumentException("set-snapshot-ref requires ref-name and snapshot-id");
                }
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
            } else if ("set-statistics".equals(action)) {
                JsonNode stats = updateNode.get("statistics");
                if (stats == null || !stats.isObject()) {
                    throw new IllegalArgumentException("set-statistics requires statistics object");
                }
                statistics.add(stats.deepCopy());
            } else if ("add-schema".equals(action)) {
                JsonNode schema = updateNode.get("schema");
                if (schema == null || !schema.isObject()) {
                    throw new IllegalArgumentException("add-schema requires schema object");
                }
                schemas.add(schema.deepCopy());
                long lastColumnId = updateNode.path("last-column-id").asLong(metadata.path("last-column-id").asLong(0));
                metadata.put("last-column-id", lastColumnId);
            } else if ("set-current-schema".equals(action)) {
                int schemaId = updateNode.path("schema-id").asInt(Integer.MIN_VALUE);
                if (schemaId == Integer.MIN_VALUE) {
                    throw new IllegalArgumentException("set-current-schema requires schema-id");
                }
                if (schemaId == -1) {
                    int latestSchemaId = metadata.path("current-schema-id").asInt(0);
                    for (JsonNode schemaNode : schemas) {
                        int candidate = schemaNode.path("schema-id").asInt(latestSchemaId);
                        if (candidate > latestSchemaId) {
                            latestSchemaId = candidate;
                        }
                    }
                    metadata.put("current-schema-id", latestSchemaId);
                } else {
                    boolean exists = false;
                    for (JsonNode schemaNode : schemas) {
                        if (schemaNode.path("schema-id").asInt(Integer.MIN_VALUE) == schemaId) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        throw new IllegalArgumentException("set-current-schema references unknown schema-id: " + schemaId);
                    }
                    metadata.put("current-schema-id", schemaId);
                }
            } else {
                throw new IllegalArgumentException("Unsupported update action: " + action);
            }
        }

        long now = System.currentTimeMillis();
        metadata.put("last-sequence-number", maxSequence);
        metadata.put("last-updated-ms", now);
        ObjectNode metadataLogEntry = OBJECT_MAPPER.createObjectNode();
        metadataLogEntry.put("timestamp-ms", now);
        metadataLogEntry.put("metadata-file", currentMetadataLocation);
        metadataLog.add(metadataLogEntry);

        String tableUuid = metadata.path("table-uuid").asText("");
        String nextMetadataLocation = nextMetadataLocation(currentMetadataLocation, tableUuid);
        updatedRoot.put("metadata-location", nextMetadataLocation);

        try {
            return OBJECT_MAPPER.writeValueAsString(updatedRoot);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize updated table metadata");
        }
    }

    private static void validateCommitRequirements(JsonNode commitRoot, ObjectNode metadata) {
        JsonNode requirements = commitRoot.get("requirements");
        if (requirements == null || requirements.isMissingNode()) {
            return;
        }
        if (!requirements.isArray()) {
            throw new IllegalArgumentException("Invalid requirements");
        }
        for (JsonNode requirement : requirements) {
            if (!requirement.isObject()) {
                throw new IllegalArgumentException("Invalid requirement entry");
            }
            String type = requirement.path("type").asText("");
            if (type.isEmpty()) {
                throw new IllegalArgumentException("Requirement type is missing");
            }
            if ("assert-create".equals(type)) {
                throw new IllegalStateException("assert-create failed");
            } else if ("assert-table-uuid".equals(type)) {
                JsonNode expectedNode = requirement.get("uuid");
                if (expectedNode == null || !expectedNode.isTextual() || expectedNode.asText("").isEmpty()) {
                    throw new IllegalArgumentException("assert-table-uuid requires non-empty uuid");
                }
                String expected = expectedNode.asText();
                String actual = metadata.path("table-uuid").asText("");
                if (!expected.equals(actual)) {
                    throw new IllegalStateException("assert-table-uuid failed");
                }
            } else if ("assert-ref-snapshot-id".equals(type)) {
                JsonNode refNode = requirement.get("ref");
                if (refNode == null || !refNode.isTextual() || refNode.asText("").isEmpty()) {
                    throw new IllegalArgumentException("assert-ref-snapshot-id requires non-empty ref");
                }
                String ref = refNode.asText();
                JsonNode expectedNode = requirement.get("snapshot-id");
                if (expectedNode == null) {
                    throw new IllegalArgumentException("assert-ref-snapshot-id requires snapshot-id");
                }
                if (!expectedNode.isNull() && !expectedNode.canConvertToLong()) {
                    throw new IllegalArgumentException("assert-ref-snapshot-id snapshot-id must be an integer or null");
                }
                JsonNode actualNode = metadata.path("refs").path(ref).get("snapshot-id");
                boolean matches = (expectedNode == null || expectedNode.isNull())
                        ? (actualNode == null || actualNode.isNull())
                        : (actualNode != null && actualNode.asLong(Long.MIN_VALUE) == expectedNode.asLong(Long.MAX_VALUE));
                if (!matches) {
                    throw new IllegalStateException("assert-ref-snapshot-id failed");
                }
            } else if ("assert-current-schema-id".equals(type)) {
                JsonNode expectedNode = requirement.get("current-schema-id");
                if (expectedNode == null || !expectedNode.canConvertToInt()) {
                    throw new IllegalArgumentException("assert-current-schema-id requires integer current-schema-id");
                }
                int expected = expectedNode.asInt(Integer.MIN_VALUE);
                int actual = metadata.path("current-schema-id").asInt(Integer.MIN_VALUE);
                if (expected == Integer.MIN_VALUE || expected != actual) {
                    throw new IllegalStateException("assert-current-schema-id failed");
                }
            } else if ("assert-last-assigned-field-id".equals(type)) {
                JsonNode expectedNode = requirement.get("last-assigned-field-id");
                if (expectedNode == null || !expectedNode.canConvertToLong()) {
                    throw new IllegalArgumentException("assert-last-assigned-field-id requires integer last-assigned-field-id");
                }
                long expected = expectedNode.asLong(Long.MIN_VALUE);
                long actual = metadata.path("last-column-id").asLong(Long.MIN_VALUE);
                if (expected == Long.MIN_VALUE || expected != actual) {
                    throw new IllegalStateException("assert-last-assigned-field-id failed");
                }
            } else if ("assert-last-assigned-partition-id".equals(type)) {
                JsonNode expectedNode = requirement.get("last-assigned-partition-id");
                if (expectedNode == null || !expectedNode.canConvertToLong()) {
                    throw new IllegalArgumentException("assert-last-assigned-partition-id requires integer last-assigned-partition-id");
                }
                long expected = expectedNode.asLong(Long.MIN_VALUE);
                long actual = metadata.path("last-partition-id").asLong(Long.MIN_VALUE);
                if (expected == Long.MIN_VALUE || expected != actual) {
                    throw new IllegalStateException("assert-last-assigned-partition-id failed");
                }
            } else if ("assert-default-spec-id".equals(type)) {
                JsonNode expectedNode = requirement.get("default-spec-id");
                if (expectedNode == null || !expectedNode.canConvertToLong()) {
                    throw new IllegalArgumentException("assert-default-spec-id requires integer default-spec-id");
                }
                long expected = expectedNode.asLong(Long.MIN_VALUE);
                long actual = metadata.path("default-spec-id").asLong(Long.MIN_VALUE);
                if (expected == Long.MIN_VALUE || expected != actual) {
                    throw new IllegalStateException("assert-default-spec-id failed");
                }
            } else {
                throw new IllegalArgumentException("Unsupported requirement type: " + type);
            }
        }
    }

    static void persistMetadataFile(String loadTableResponseJson) {
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

    static String extractMetadataLocation(String loadTableResponseJson) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(loadTableResponseJson);
            String metadataLocation = root.path("metadata-location").asText("");
            if (metadataLocation.isEmpty()) {
                throw new IllegalArgumentException("Missing metadata-location");
            }
            return metadataLocation;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON response payload");
        }
    }

    static String loadTableResponseFromMetadataLocation(String metadataLocation) {
        try {
            Path metadataPath = resolveWritablePath(metadataLocation);
            if (metadataPath == null) {
                throw new IllegalArgumentException("Unsupported metadata-location scheme: " + metadataLocation);
            }
            String metadataContent = Files.readString(metadataPath, StandardCharsets.UTF_8);
            JsonNode metadata = OBJECT_MAPPER.readTree(metadataContent);
            ObjectNode response = OBJECT_MAPPER.createObjectNode();
            response.put("metadata-location", metadataLocation);
            response.set("metadata", metadata);
            response.putObject("config");
            return OBJECT_MAPPER.writeValueAsString(response);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read metadata file: " + metadataLocation, e);
        }
    }

    private static String nextMetadataLocation(String currentMetadataLocation, String tableUuid) {
        URI uri = URI.create(currentMetadataLocation);
        Path currentPath = Paths.get(uri.getPath());
        String fileName = currentPath.getFileName().toString();
        Matcher matcher = METADATA_FILE_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid metadata file name: " + fileName);
        }

        int currentVersion = Integer.parseInt(matcher.group(1));
        String effectiveUuid = (tableUuid == null || tableUuid.isEmpty()) ? matcher.group(2) : tableUuid;
        String nextFileName = String.format("%05d-%s.metadata.json", currentVersion + 1, effectiveUuid);
        Path nextPath = currentPath.getParent().resolve(nextFileName);

        return URI.create(uri.getScheme() + "://" + nextPath.toString()).toString();
    }

    static Path resolveWritablePath(String location) {
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

    private static ArrayNode ensureArray(ObjectNode parent, String fieldName) {
        JsonNode node = parent.get(fieldName);
        if (node == null || !node.isArray()) {
            return parent.putArray(fieldName);
        }
        return (ArrayNode) node;
    }

    private static ObjectNode ensureObject(ObjectNode parent, String fieldName) {
        JsonNode node = parent.get(fieldName);
        if (node == null || !node.isObject()) {
            return parent.putObject(fieldName);
        }
        return (ObjectNode) node;
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
                if ("POST".equals(method) && "/v1/tables/rename".equals(path)) {
                    handleRenameTable(exchange, method, path);
                    return;
                }

                if ("POST".equals(method) && path.startsWith("/v1/namespaces/") && path.endsWith("/properties")) {
                    handleUpdateNamespaceProperties(exchange, method, path);
                    return;
                }

                if (path.startsWith(TABLE_PATH_PREFIX) && path.contains(TABLES_SEGMENT)) {
                    handleTableRoutes(exchange, method, path);
                    return;
                }

                if ("GET".equals(method) || "DELETE".equals(method)) {
                    handleGetNamespace(exchange, method, path, exchange.getRequestURI().getQuery());
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

        private void handleUpdateNamespaceProperties(HttpExchange exchange, String method, String path) throws IOException {
            String namespace = path.substring("/v1/namespaces/".length(), path.length() - "/properties".length());
            if (namespace.endsWith("/")) {
                namespace = namespace.substring(0, namespace.length() - 1);
            }
            if (namespace.isEmpty() || namespace.contains("/")) {
                sendIcebergError(exchange, 404, "Not Found", method, path);
                return;
            }
            if (!namespaceStore.listNamespaces().contains(namespace)) {
                sendIcebergError(exchange, 404, "Namespace not found: " + namespace, method, path);
                return;
            }

            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root;
            try {
                root = OBJECT_MAPPER.readTree(requestBody);
            } catch (JsonProcessingException e) {
                sendIcebergError(exchange, 400, "Invalid request body", method, path);
                return;
            }
            JsonNode updatesNode = root.path("updates");
            JsonNode removalsNode = root.path("removals");
            if (!updatesNode.isObject() || !removalsNode.isArray()) {
                sendIcebergError(exchange, 400, "Invalid properties update payload", method, path);
                return;
            }

            List<String> updated = new ArrayList<>();
            List<String> removed = new ArrayList<>();
            List<String> missing = new ArrayList<>();
            synchronized (namespaceProperties) {
                Map<String, String> properties = namespaceProperties.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>());
                updatesNode.fields().forEachRemaining(entry -> {
                    properties.put(entry.getKey(), entry.getValue().asText(""));
                    updated.add(entry.getKey());
                });
                for (JsonNode removal : removalsNode) {
                    String key = removal.asText("");
                    if (key.isEmpty()) {
                        continue;
                    }
                    if (properties.containsKey(key)) {
                        properties.remove(key);
                        removed.add(key);
                    } else {
                        missing.add(key);
                    }
                }
            }

            String body = "{\"updated\":" + stringArrayJson(updated)
                    + ",\"removed\":" + stringArrayJson(removed)
                    + ",\"missing\":" + stringArrayJson(missing) + "}";
            sendJson(exchange, 200, body, method, path);
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

            if ("DELETE".equals(method) && parts.length == 3) {
                handleDeleteTable(exchange, method, path, namespace, parts[2]);
                return;
            }

            if ("POST".equals(method) && parts.length == 4 && "metrics".equals(parts[3])) {
                sendNoContent(exchange, method, path);
                return;
            }

            sendIcebergError(exchange, 404, "Not Found", method, path);
        }

        private void handleGetNamespace(HttpExchange exchange, String method, String path, String query) throws IOException {
            if ("/v1/namespaces".equals(path) || "/v1/namespaces/".equals(path)) {
                sendJson(exchange, 200, listNamespacesJson(readQueryParam(query, "parent")), method, path);
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

            if ("DELETE".equals(method)) {
                handleDeleteNamespace(exchange, method, path, suffix);
                return;
            }

            if (!namespaceStore.listNamespaces().contains(suffix)) {
                sendIcebergError(exchange, 404, "Namespace not found: " + suffix, method, path);
                return;
            }

            sendJson(exchange, 200, namespaceObjectJson(suffix), method, path);
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
                loadTableResponseJson = IcebergRestServer.buildLoadTableResponseJson(namespace, requestBody);
            } catch (IllegalArgumentException e) {
                sendIcebergError(exchange, 400, e.getMessage(), method, path);
                return;
            }

            JsonNode root;
            try {
                root = OBJECT_MAPPER.readTree(requestBody);
            } catch (JsonProcessingException e) {
                sendIcebergError(exchange, 400, "Invalid JSON request body", method, path);
                return;
            }
            String tableName = root.path("name").asText();
            if (tableName == null || tableName.isEmpty()) {
                sendIcebergError(exchange, 400, "Missing table name", method, path);
                return;
            }
            if (tableStore.getTableResponse(namespace, tableName) != null) {
                sendIcebergError(exchange, 409, "Table already exists: " + namespace + "." + tableName, method, path);
                return;
            }
            String normalizedResponseJson = normalizeMetadataLocation(loadTableResponseJson);
            boolean stageCreate = root.path("stage-create").asBoolean(false);
            if (!stageCreate) {
                tableStore.putTableResponse(namespace, tableName, normalizedResponseJson);
                IcebergRestServer.persistMetadataFile(normalizedResponseJson);
            }
            sendJson(exchange, 200, normalizedResponseJson, method, path);
        }

        private void handleCommitTable(HttpExchange exchange, String method, String path, String namespace, String table) throws IOException {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Request body: " + requestBody);
            String updatedResponseJson;
            try {
                updatedResponseJson = tableStore.commitTable(namespace, table, requestBody);
            } catch (TableStore.TableNotFoundException e) {
                sendIcebergError(exchange, 404, e.getMessage(), method, path);
                return;
            } catch (IllegalArgumentException e) {
                sendIcebergError(exchange, 400, e.getMessage(), method, path);
                return;
            } catch (IllegalStateException e) {
                sendIcebergError(exchange, 409, e.getMessage(), method, path);
                return;
            }

            String normalizedResponseJson = normalizeMetadataLocation(updatedResponseJson);
            sendJson(exchange, 200, normalizedResponseJson, method, path);
        }

        private void handleCreateNamespace(HttpExchange exchange, String method, String path) throws IOException {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Request body: " + requestBody);
            JsonNode root;
            try {
                root = OBJECT_MAPPER.readTree(requestBody);
            } catch (JsonProcessingException e) {
                sendIcebergError(exchange, 400, "Invalid request body", method, path);
                return;
            }
            JsonNode nsNode = root.path("namespace");
            if (!nsNode.isArray() || nsNode.isEmpty()) {
                sendIcebergError(exchange, 400, "Invalid namespace", method, path);
                return;
            }
            List<String> nsParts = new ArrayList<>();
            for (JsonNode part : nsNode) {
                String value = part.asText("").trim();
                if (value.isEmpty()) {
                    sendIcebergError(exchange, 400, "Invalid namespace", method, path);
                    return;
                }
                nsParts.add(value);
            }
            String namespaceValue = String.join(".", nsParts);
            namespaceStore.createNamespace(namespaceValue);

            Map<String, String> properties = new LinkedHashMap<>();
            JsonNode props = root.path("properties");
            if (props.isObject()) {
                props.fields().forEachRemaining(entry -> properties.put(entry.getKey(), entry.getValue().asText("")));
            }
            synchronized (namespaceProperties) {
                namespaceProperties.put(namespaceValue, properties);
            }
            sendJson(exchange, 200, namespaceObjectJson(namespaceValue), method, path);
        }

        private void handleDeleteNamespace(HttpExchange exchange, String method, String path, String namespace) throws IOException {
            if (!namespaceStore.listNamespaces().contains(namespace)) {
                sendIcebergError(exchange, 404, "Namespace not found: " + namespace, method, path);
                return;
            }
            if (!tableStore.listTables(namespace).isEmpty()) {
                sendIcebergError(exchange, 409, "Namespace not empty: " + namespace, method, path);
                return;
            }
            String prefix = namespace + ".";
            for (String existing : namespaceStore.listNamespaces()) {
                if (existing.startsWith(prefix)) {
                    sendIcebergError(exchange, 409, "Namespace has children: " + namespace, method, path);
                    return;
                }
            }
            namespaceStore.deleteNamespace(namespace);
            synchronized (namespaceProperties) {
                namespaceProperties.remove(namespace);
            }
            sendNoContent(exchange, method, path);
        }

        private void handleDeleteTable(HttpExchange exchange, String method, String path, String namespace, String table) throws IOException {
            if (!tableStore.deleteTable(namespace, table)) {
                sendIcebergError(exchange, 404, "Table not found: " + namespace + "." + table, method, path);
                return;
            }
            sendNoContent(exchange, method, path);
        }

        private void handleRenameTable(HttpExchange exchange, String method, String path) throws IOException {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Request body: " + requestBody);
            JsonNode root;
            try {
                root = OBJECT_MAPPER.readTree(requestBody);
            } catch (JsonProcessingException e) {
                sendIcebergError(exchange, 400, "Invalid request body", method, path);
                return;
            }
            JsonNode source = root.path("source");
            JsonNode destination = root.path("destination");
            String sourceNamespace = namespaceFromNode(source.path("namespace"));
            String destinationNamespace = namespaceFromNode(destination.path("namespace"));
            String sourceName = source.path("name").asText("");
            String destinationName = destination.path("name").asText("");
            if (sourceNamespace == null || destinationNamespace == null || sourceName.isEmpty() || destinationName.isEmpty()) {
                sendIcebergError(exchange, 400, "Invalid rename payload", method, path);
                return;
            }
            if (!namespaceStore.listNamespaces().contains(sourceNamespace) || !namespaceStore.listNamespaces().contains(destinationNamespace)) {
                sendIcebergError(exchange, 404, "Namespace not found", method, path);
                return;
            }
            if (!tableStore.renameTable(sourceNamespace, sourceName, destinationNamespace, destinationName)) {
                sendIcebergError(exchange, 404, "Table not found: " + sourceNamespace + "." + sourceName, method, path);
                return;
            }
            sendNoContent(exchange, method, path);
        }

        private String namespaceFromNode(JsonNode nsNode) {
            if (!nsNode.isArray() || nsNode.isEmpty()) {
                return null;
            }
            List<String> parts = new ArrayList<>();
            for (JsonNode part : nsNode) {
                String value = part.asText("").trim();
                if (value.isEmpty()) {
                    return null;
                }
                parts.add(value);
            }
            return String.join(".", parts);
        }

        private String readQueryParam(String query, String key) {
            if (query == null || query.isEmpty()) {
                return null;
            }
            String[] pieces = query.split("&");
            for (String piece : pieces) {
                int idx = piece.indexOf('=');
                if (idx <= 0) {
                    continue;
                }
                String param = piece.substring(0, idx);
                String value = piece.substring(idx + 1);
                if (key.equals(param) && !value.isBlank()) {
                    return value;
                }
            }
            return null;
        }

        private String normalizeMetadataLocation(String loadTableResponseJson) {
            return loadTableResponseJson;
        }

        private static String listNamespacesJson(String parent) {
            List<String> namespaces = namespaceStore.listNamespaces();
            StringBuilder builder = new StringBuilder();
            builder.append("{\"namespaces\":[");
            String parentPrefix = parent == null ? null : parent + ".";
            int parentDepth = parent == null ? 0 : parent.split("\\.").length;
            int written = 0;
            for (int i = 0; i < namespaces.size(); i++) {
                String namespace = namespaces.get(i);
                if (parent != null) {
                    if (!namespace.startsWith(parentPrefix)) {
                        continue;
                    }
                    if (namespace.split("\\.").length != parentDepth + 1) {
                        continue;
                    }
                }
                if (written > 0) {
                    builder.append(",");
                }
                builder.append(namespaceArrayJson(namespace));
                written++;
            }
            builder.append("]}");
            return builder.toString();
        }

        private static String namespaceArrayJson(String namespace) {
            String[] parts = namespace.split("\\.");
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    builder.append(",");
                }
                builder.append("\"").append(escapeJson(parts[i])).append("\"");
            }
            builder.append("]");
            return builder.toString();
        }

        private static String namespaceObjectJson(String namespace) {
            StringBuilder builder = new StringBuilder();
            builder.append("{\"namespace\":")
                    .append(namespaceArrayJson(namespace))
                    .append(",\"properties\":");
            Map<String, String> properties;
            synchronized (namespaceProperties) {
                properties = namespaceProperties.getOrDefault(namespace, Map.of());
            }
            builder.append("{");
            int i = 0;
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (i++ > 0) {
                    builder.append(",");
                }
                builder.append("\"").append(escapeJson(entry.getKey())).append("\":\"")
                        .append(escapeJson(entry.getValue())).append("\"");
            }
            builder.append("}}");
            return builder.toString();
        }

        private static String stringArrayJson(List<String> values) {
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    builder.append(",");
                }
                builder.append("\"").append(escapeJson(values.get(i))).append("\"");
            }
            builder.append("]");
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
