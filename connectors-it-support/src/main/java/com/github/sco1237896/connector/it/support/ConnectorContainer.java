package com.github.sco1237896.connector.it.support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.CaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.StopContainerCmd;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;

public class ConnectorContainer extends GenericContainer<ConnectorContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorContainer.class);

    public static final String DEFAULT_APPLICATION_PROPERTIES_LOCATION = "/etc/camel/application.properties";
    public static final String DEFAULT_USER_PROPERTIES_LOCATION = "/etc/camel/conf.d/user.properties";
    public static final String DEFAULT_ROUTE_LOCATION = "/etc/camel/sources/route.yaml";

    public static final String CONTAINER_ALIAS = "tc-connector";
    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final int GRACEFUL_STOP_TIMEOUT = 30;

    private Consumer<ConnectorContainer> customizer;
    private final List<Pair<String, byte[]>> files;
    private final Map<String, String> userProperties;

    public ConnectorContainer(String image) {
        this(DockerImageName.parse(image));
    }

    public ConnectorContainer(DockerImageName imageName) {
        super(imageName);

        this.files = new ArrayList<>();
        this.userProperties = new TreeMap<>();

        withEnv("QUARKUS_LOG_CONSOLE_JSON", "false");
        withEnv("CAMEL_K_MOUNT_PATH_CONFIGMAPS", "/etc/camel/conf.d/_configmaps");
        withEnv("CAMEL_K_MOUNT_PATH_SECRETS", "/etc/camel/conf.d/_secrets");
        withEnv("CAMEL_K_CONF_D", "/etc/camel/conf.d");
        withEnv("CAMEL_K_CONF", "/etc/camel/application.properties");

        withExposedPorts(DEFAULT_HTTP_PORT);
        waitingFor(WaitStrategies.forHealth(DEFAULT_HTTP_PORT));
        withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(CONTAINER_ALIAS)));
    }

    public ConnectorContainer withCustomizer(Consumer<ConnectorContainer> customizer) {
        this.customizer = customizer;
        return self();

    }

    public ConnectorContainer withUserProperties(Map<String, String> properties) {
        this.userProperties.putAll(properties);
        return self();
    }

    public ConnectorContainer withUserProperty(String key, String format, Object... args) {
        this.userProperties.put(
                key,
                args.length == 0
                        ? format
                        : String.format(format, args));

        return self();
    }

    public ConnectorContainer withFile(String path, InputStream content) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(content);

        return withFile(path, content.readAllBytes());
    }

    public ConnectorContainer withFile(String path, byte[] content) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(content);

        this.files.add(new ImmutablePair<>(path, content));

        return self();
    }

    public ConnectorContainer withFile(String path, String content) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(content);

        this.files.add(new ImmutablePair<>(path, content.getBytes(StandardCharsets.UTF_8)));

        return self();
    }

    public String getServiceAddress() {
        return getHost();
    }

    public int getServicePort() {
        return getMappedPort(DEFAULT_HTTP_PORT);
    }

    public RequestSpecification getRequest() {
        return RestAssured.given()
                .baseUri("http://" + getServiceAddress())
                .port(this.getServicePort());
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        super.containerIsStarting(containerInfo);

        if (this.customizer != null) {
            this.customizer.accept(this);
        }

        for (Pair<String, byte[]> file : files) {
            copyFileToContainer(Transferable.of(file.getRight()), file.getLeft());
        }

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            Properties p = new Properties();

            try (InputStream ip = ConnectorContainer.class.getResourceAsStream("/integration-user.properties")) {
                p.load(ip);
            }

            p.putAll(this.userProperties);
            p.store(os, "user");

            copyFileToContainer(Transferable.of(os.toByteArray()), DEFAULT_USER_PROPERTIES_LOCATION);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void containerIsStopping(InspectContainerResponse containerInfo) {
        super.containerIsStopped(containerInfo);
        LOGGER.info("Container is stopping. Attempting to wait for graceful stop for {} seconds.", GRACEFUL_STOP_TIMEOUT);
        try (StopContainerCmd stopContainerCmd = getDockerClient()
                .stopContainerCmd(getContainerId())
                .withTimeout(GRACEFUL_STOP_TIMEOUT)) {
            stopContainerCmd.exec();
            LOGGER.info("Container was gracefully stopped.");
        } catch (Exception e) {
            LOGGER.error("Failed to gracefully stop container, this might lead to resource leaking.", e);
        }
    }

    public static Builder forDefinition(String definition) {
        return new Builder(definition);
    }

    public void withCamelComponentDebugEnv() {
        withEnv("quarkus.log.level", "DEBUG");
        withEnv("quarkus.log.category.\"org.apache.camel.component\".level", "DEBUG");
    }

    public static class Builder {
        private final Path definition;
        private final Map<String, Object> properties;
        private final Map<String, String> userProperties;
        private final Map<String, Map<String, Object>> kameletProperties;

        private Network network;

        private Builder(String definition) {
            String root = System.getProperty("connectors.catalog.definition.root");

            Objects.requireNonNull(definition);
            Objects.requireNonNull(root);

            if (!definition.startsWith(root)) {
                this.definition = Path.of(root, definition);
            } else {
                this.definition = Path.of(definition);
            }

            this.properties = new TreeMap<>();
            this.userProperties = new TreeMap<>();
            this.kameletProperties = new TreeMap<>();
        }

        public Builder withSourceProperties(Map<String, Object> properties) {
            Objects.requireNonNull(properties);

            this.kameletProperties.computeIfAbsent("source", k -> new TreeMap<>());
            this.kameletProperties.get("source").putAll(properties);

            return this;
        }

        public Builder withSinkProperties(Map<String, Object> properties) {
            Objects.requireNonNull(properties);

            this.kameletProperties.computeIfAbsent("sink", k -> new TreeMap<>());
            this.kameletProperties.get("sink").putAll(properties);

            return this;
        }

        public Builder withProperty(String key, String val) {
            Objects.requireNonNull(key);
            Objects.requireNonNull(val);

            this.properties.put(key.trim(), val);
            return this;
        }

        public Builder withProperties(Map<String, Object> properties) {
            Objects.requireNonNull(properties);

            this.properties.putAll(properties);
            return this;
        }

        public Builder withUserProperty(String key, String val) {
            Objects.requireNonNull(key);
            Objects.requireNonNull(val);

            this.userProperties.put(key.trim(), val);
            return this;
        }

        public Builder withUserProperties(Map<String, String> properties) {
            Objects.requireNonNull(properties);

            this.userProperties.putAll(properties);
            return this;
        }

        public Builder withLogCategory(String category, String level) {
            Objects.requireNonNull(category);
            Objects.requireNonNull(level);

            return withUserProperty(
                    String.format("quarkus.log.category.\"%s\".level", category),
                    level);
        }

        public Builder witNetwork(Network network) {
            this.network = network;
            return this;
        }

        public ConnectorContainer build() {
            try (InputStream is = Files.newInputStream(definition)) {
                ObjectMapper yaml = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

                ObjectMapper mapper = new YAMLMapper();
                ObjectNode def = mapper.readValue(is, ObjectNode.class);

                String image = def.requiredAt("/definition/metadata/annotations").get("trait.camel.apache.org/container.image")
                        .asText();
                DockerImageName imageName = DockerImageName.parse(image);

                ConnectorContainer answer = new ConnectorContainer(imageName);

                if (!kameletProperties.isEmpty()) {
                    String sourceKamelet = def.requiredAt("/definition/spec/source/ref/name").asText();
                    String sinkKamelet = def.requiredAt("/definition/spec/sink/ref/name").asText();

                    if (sourceKamelet.equals("connector-kafka-source")) {
                        sourceKamelet = "connector-kafka-not-secured-source";
                    }
                    if (sinkKamelet.equals("connector-kafka-sink")) {
                        sinkKamelet = "connector-kafka-not-secured-sink";
                    }

                    ArrayNode integration = yaml.createArrayNode();
                    ObjectNode route = integration.addObject().with("route");

                    ObjectNode from = route.with("from");
                    from.put("uri", "kamelet:" + sourceKamelet);

                    if (kameletProperties.containsKey("source")) {
                        for (var entry : kameletProperties.get("source").entrySet()) {
                            from.with("parameters").put(entry.getKey(), entry.getValue().toString());
                        }
                    }

                    ArrayNode steps = from.withArray("steps");
                    steps.addObject().with("to").put("uri", "log:steps-begin?showAll=true&multiline=true");
                    steps.addObject().with("removeHeader").put("name", "X-Content-Schema");
                    steps.addObject().with("removeProperty").put("name", "X-Content-Schema");
                    steps.addObject().with("to").put("uri", "log:steps-end?showAll=true&multiline=true");

                    ObjectNode to = steps.addObject().with("to");
                    to.put("uri", "kamelet:" + sinkKamelet);

                    if (kameletProperties.containsKey("sink")) {
                        for (var entry : kameletProperties.get("sink").entrySet()) {
                            to.with("parameters").put(entry.getKey(), entry.getValue().toString());
                        }
                    }

                    // add this log to trace what happens after the message gets delivered
                    // to the target endpoint for troubleshooting purpose.
                    //
                    // i.e. the exchange will contain headers and properties added by the
                    // target system component
                    steps.addObject().with("to").put("uri", "log:after?showAll=true&multiline=true");

                    String routeYaml = yaml.writerWithDefaultPrettyPrinter().writeValueAsString(integration);

                    LOGGER.info("\n\n----------------\nroute: \n{}\n----------------\n\n", routeYaml);

                    answer.withFile(DEFAULT_ROUTE_LOCATION, routeYaml);

                    LOGGER.info("\n\n----------------\nuser properties: \n{}\n----------------\n\n", userProperties);
                    answer.withUserProperties(userProperties);

                    try (InputStream ip = ConnectorContainer.class.getResourceAsStream("/integration-application.properties")) {
                        if (ip == null) {
                            throw new IllegalStateException("Unable to read integration-application.properties");
                        }

                        byte[] bytes = ip.readAllBytes();

                        LOGGER.info("\n\n----------------\napplication properties: \n{}\n----------------\n\n",
                                new String(bytes));

                        answer.withFile(DEFAULT_APPLICATION_PROPERTIES_LOCATION, new ByteArrayInputStream(bytes));
                    }
                }

                if (this.network != null) {
                    answer.withNetwork(this.network);
                }

                return answer;

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
