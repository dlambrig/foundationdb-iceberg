import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class MetadataFileSupport {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Path DEFAULT_LOCAL_ROOT = Paths.get(System.getProperty("java.io.tmpdir"));
    private static final Pattern METADATA_FILE_PATTERN = Pattern.compile("^(\\d+)-(.+)\\.metadata\\.json$");
    private static final String WRITE_METADATA_DELETE_AFTER_COMMIT_ENABLED = "write.metadata.delete-after-commit.enabled";

    private MetadataFileSupport() {
    }

    static void persistMetadataFile(String responseJson) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseJson);
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

    static String extractMetadataLocation(String responseJson) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseJson);
            String metadataLocation = root.path("metadata-location").asText("");
            if (metadataLocation.isEmpty()) {
                throw new IllegalArgumentException("Missing metadata-location");
            }
            return metadataLocation;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON response payload");
        }
    }

    static String loadCatalogResponseFromMetadataLocation(String metadataLocation) {
        try {
            Path metadataPath = resolveWritablePath(metadataLocation);
            if (metadataPath == null) {
                throw new IllegalArgumentException("Unsupported metadata-location scheme: " + metadataLocation);
            }
            String metadataContent = Files.readString(metadataPath, StandardCharsets.UTF_8);
            JsonNode metadata = OBJECT_MAPPER.readTree(metadataContent);
            var response = OBJECT_MAPPER.createObjectNode();
            response.put("metadata-location", metadataLocation);
            response.set("metadata", metadata);
            response.putObject("config");
            return OBJECT_MAPPER.writeValueAsString(response);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read metadata file: " + metadataLocation, e);
        }
    }

    static List<String> collectMetadataFilesToDeleteAfterCommit(String existingResponseJson, String updatedResponseJson) {
        try {
            JsonNode existingRoot = OBJECT_MAPPER.readTree(existingResponseJson);
            JsonNode updatedRoot = OBJECT_MAPPER.readTree(updatedResponseJson);

            if (!isMetadataDeleteAfterCommitEnabled(updatedRoot.path("metadata").path("properties"))) {
                return Collections.emptyList();
            }

            Set<String> oldRefs = collectTrackedMetadataFiles(existingRoot);
            Set<String> newRefs = collectTrackedMetadataFiles(updatedRoot);
            oldRefs.removeAll(newRefs);
            if (oldRefs.isEmpty()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(oldRefs);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON response payload");
        }
    }

    static void deleteMetadataFilesQuietly(List<String> metadataLocations) {
        for (String metadataLocation : metadataLocations) {
            try {
                Path metadataPath = resolveWritablePath(metadataLocation);
                if (metadataPath != null) {
                    Files.deleteIfExists(metadataPath);
                }
            } catch (Exception ignored) {
                // Best-effort cleanup; commit metadata pointer changes must not be rolled back.
            }
        }
    }

    static String nextMetadataLocation(String currentMetadataLocation, String objectUuid) {
        URI uri = URI.create(currentMetadataLocation);
        Path currentPath = Paths.get(uri.getPath());
        String fileName = currentPath.getFileName().toString();
        Matcher matcher = METADATA_FILE_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid metadata file name: " + fileName);
        }

        int currentVersion = Integer.parseInt(matcher.group(1));
        String effectiveUuid = (objectUuid == null || objectUuid.isEmpty()) ? matcher.group(2) : objectUuid;
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

    private static boolean isMetadataDeleteAfterCommitEnabled(JsonNode properties) {
        if (properties == null || properties.isMissingNode()) {
            return false;
        }
        JsonNode enabledNode = properties.get(WRITE_METADATA_DELETE_AFTER_COMMIT_ENABLED);
        return enabledNode != null && enabledNode.isTextual() && Boolean.parseBoolean(enabledNode.asText());
    }

    private static Set<String> collectTrackedMetadataFiles(JsonNode responseRoot) {
        Set<String> refs = new LinkedHashSet<>();
        String current = responseRoot.path("metadata-location").asText("");
        if (!current.isEmpty()) {
            refs.add(current);
        }

        JsonNode metadataLog = responseRoot.path("metadata").path("metadata-log");
        if (metadataLog.isArray()) {
            for (JsonNode entry : metadataLog) {
                String metadataFile = entry.path("metadata-file").asText("");
                if (!metadataFile.isEmpty()) {
                    refs.add(metadataFile);
                }
            }
        }
        return refs;
    }
}
