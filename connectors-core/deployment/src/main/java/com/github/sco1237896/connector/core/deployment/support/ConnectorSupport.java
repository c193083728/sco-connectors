package com.github.sco1237896.connector.core.deployment.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.dockerjava.zerodep.shaded.org.apache.commons.codec.digest.DigestUtils;
import com.github.sco1237896.connector.core.deployment.ConnectorConfiguration;
import com.github.sco1237896.connector.core.deployment.spi.KameletBuildItem;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.quarkus.maven.dependency.ResolvedDependency;
import net.javacrumbs.jsonunit.core.Configuration;
import net.javacrumbs.jsonunit.core.internal.Diff;
import net.javacrumbs.jsonunit.core.internal.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class ConnectorSupport {
    public static final String API_GROUP = "sco1237896.github.com";
    public static final String API_VERSION = "v1";
    public static final String CAMEL_API_GROUP = "camel.apache.org";
    public static final String CAMEL_API_VERSION = "v1";

    public static final String KAMELETS_BASE_PATH = "kamelets";
    public static final String KAMELETS_EXTENSION = ".kamelet.yaml";

    public static final String CONNECTOR_REVISION = "sco1237896.github.com/connector.revision";

    public static final String TRAIT_CAMEL_APACHE_ORG_ENV = "trait.camel.apache.org/environment.%s";
    public static final String TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE = "trait.camel.apache.org/container.image";
    public static final String TRAIT_CAMEL_APACHE_ORG_CONTAINER_REQUEST_CPU = "trait.camel.apache.org/container.request-cpu";
    public static final String TRAIT_CAMEL_APACHE_ORG_CONTAINER_REQUEST_MEMORY = "trait.camel.apache.org/container.request-memory";
    public static final String TRAIT_CAMEL_APACHE_ORG_PROMETHEUS_ENABLED = "trait.camel.apache.org/prometheus.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_PROMETHEUS_POD_MONITOR = "trait.camel.apache.org/prometheus.pod-monitor";
    public static final String TRAIT_CAMEL_APACHE_ORG_KAMELETS_ENABLED = "trait.camel.apache.org/kamelets.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_JVM_ENABLED = "trait.camel.apache.org/jvm.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_LOGGING_JSON = "trait.camel.apache.org/logging.json";
    public static final String TRAIT_CAMEL_APACHE_ORG_OWNER_TARGET_LABELS = "trait.camel.apache.org/owner.target-labels";
    public static final String TRAIT_CAMEL_APACHE_ORG_OWNER_TARGET_ANNOTATIONS = "trait.camel.apache.org/owner.target-annotations";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_ENABLED = "trait.camel.apache.org/health.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_PROBE_ENABLED = "trait.camel.apache.org/health.liveness-probe-enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_PERIOD = "trait.camel.apache.org/health.liveness-period";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_TIMEOUT = "trait.camel.apache.org/health.liveness-timeout";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_SUCCESS_THRESHOLD = "trait.camel.apache.org/health.liveness-success-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_FAILURE_THRESHOLD = "trait.camel.apache.org/health.liveness-failure-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_PROBE_ENABLED = "trait.camel.apache.org/health.readiness-probe-enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_PERIOD = "trait.camel.apache.org/health.readiness-period";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_TIMEOUT = "trait.camel.apache.org/health.readiness-timeout";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_SUCCESS_THRESHOLD = "trait.camel.apache.org/health.readiness-success-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_FAILURE_THRESHOLD = "trait.camel.apache.org/health.readiness-failure-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_DEPLOYMENT_ENABLED = "trait.camel.apache.org/deployment.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_DEPLOYMENT_STRATEGY = "trait.camel.apache.org/deployment.strategy";
    public static final String TRAIT_CAMEL_APACHE_ORG_DEPLOYMENT_PROGRESS_DEADLINE = "trait.camel.apache.org/deployment.progress-deadline-seconds";

    public static final ObjectMapper MAPPER = new YAMLMapper()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
            .configure(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE, true)
            .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, false);

    public static final ObjectWriter WRITER = MAPPER.writerWithDefaultPrettyPrinter();

    private ConnectorSupport() {
    }

    public static ObjectNode readValue(Path path) {
        try (InputStream is = Files.newInputStream(path)) {
            return MAPPER.readValue(is, ObjectNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String kameletName(ObjectNode node) {
        return node.requiredAt("/metadata/name").asText();
    }

    public static String kameletVersion(ObjectNode node) {
        JsonNode annotations = node.requiredAt("/metadata/annotations");
        JsonNode version = annotations.get("camel.apache.org/kamelet.version");
        if (version == null) {
            version = annotations.get("camel.apache.org/catalog.version");
        }

        if (version == null) {
            return null;
        }

        return version.asText();
    }

    public static Optional<ObjectNode> lookupKamelet(
            ConnectorConfiguration.KameletRef ref,
            List<KameletBuildItem> kamelets) {

        for (KameletBuildItem item : kamelets) {
            final String name = ConnectorSupport.kameletName(item.getDefinition());
            final String version = ConnectorSupport.kameletVersion(item.getDefinition());

            if (Objects.equals(ref.name(), name) && Objects.equals(ref.version(), version)) {
                return Optional.of(item.getDefinition());
            }
        }

        return Optional.empty();
    }

    public static ObjectNode cleanupKamelet(ObjectNode kamelet) {
        ObjectNode answer = kamelet.deepCopy();

        JsonNode meta = answer.at("/metadata");
        if (meta.isObject()) {
            ((ObjectNode) meta).remove("annotations");
            ((ObjectNode) meta).remove("labels");
        }

        JsonNode spec = answer.at("/spec");
        if (spec.isObject()) {
            ((ObjectNode) spec).remove("template");
            ((ObjectNode) spec).remove("dependencies");
        }

        return answer;
    }

    public static String computeSha256Digest(ResolvedDependency dependency) {
        MessageDigest digest = DigestUtils.getSha256Digest();

        if (dependency.getKey().getGroupId().startsWith("com.github.sco1237896")) {
            for (Path path : dependency.getResolvedPaths()) {
                if (!Files.isRegularFile(path)) {
                    continue;
                }
                if (!path.toString().endsWith(".jar")) {
                    continue;
                }

                try {
                    computeQuarkusArchiveDigest(path, digest);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            DigestUtils.updateDigest(digest, dependency.getKey().toGacString());
        }

        return DigestUtils.sha256Hex(digest.digest());
    }

    public static void computeQuarkusArchiveDigest(Path path, MessageDigest digest) throws Exception {
        try (JarFile jar = new JarFile(path.toFile())) {
            List<JarEntry> entries = Collections.list(jar.entries());
            entries.sort(Comparator.comparing(JarEntry::getName));

            for (JarEntry entry : entries) {
                if (entry.isDirectory()) {
                    continue;
                }
                if (entry.getName().equals("META-INF/jandex.idx")) {
                    continue;
                }
                if (entry.getName().startsWith("META-INF/quarkus-")) {
                    continue;
                }
                if (entry.getName().endsWith("git.properties")) {
                    continue;
                }

                // include kamelets names to ensure kamelets renaming would trigger
                // a new connector being generated
                if (entry.getName().endsWith(KAMELETS_EXTENSION)) {
                    DigestUtils.updateDigest(digest, entry.getName());
                }

                try (InputStream is = jar.getInputStream(entry)) {
                    DigestUtils.updateDigest(digest, is);
                }
            }
        }
    }

    public static void removeField(ObjectNode root, String path, String field) {
        JsonNode node = root.at(path);
        if (!node.isMissingNode() && node.isObject()) {
            ((ObjectNode) node).remove(field);
        }
    }

    public static void removeFields(ObjectNode root, String path, Collection<String> fields) {
        for (String field : fields) {
            removeField(root, path, field);
        }
    }

    public static String getConnectorId(HasMetadata object) {
        return getConnectorId(object.getMetadata());
    }

    public static String getConnectorId(ObjectMeta meta) {
        return meta.getAnnotations().get(ConnectorSupport.API_GROUP + "/connector.id");
    }

    public static boolean hasConnectorId(HasMetadata object, String id) {
        if (Objects.isNull(object) || Objects.isNull(id)) {
            return false;
        }

        return Objects.equals(
                id,
                getConnectorId(object));
    }

    public static String getCatalogId(HasMetadata object) {
        return getCatalogId(object.getMetadata());
    }

    public static String getCatalogId(ObjectMeta meta) {
        return meta.getAnnotations().get(ConnectorSupport.API_GROUP + "/catalog.id");
    }

    public static boolean hasCatalogId(HasMetadata object, String id) {
        if (Objects.isNull(object) || Objects.isNull(id)) {
            return false;
        }

        return Objects.equals(
                id,
                getCatalogId(object));
    }

    public static Diff diff(ObjectNode expected, ObjectNode actual, Function<Configuration, Configuration> cc) {
        Configuration configuration = Configuration.empty();
        configuration = cc.apply(configuration);

        return Diff.create(
                expected,
                actual,
                "fullJson",
                net.javacrumbs.jsonunit.core.internal.Path.create("", JsonUtils.getPathPrefix(expected)),
                configuration);
    }

    public static boolean equals(ResolvedDependency dep1, ResolvedDependency dep2) {
        return Objects.equals(dep1.getGroupId(), dep2.getGroupId())
                && Objects.equals(dep1.getArtifactId(), dep2.getArtifactId());
    }
}
