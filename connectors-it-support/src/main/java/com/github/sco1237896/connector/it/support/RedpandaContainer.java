package com.github.sco1237896.connector.it.support;

import com.github.dockerjava.api.command.InspectContainerResponse;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class RedpandaContainer extends GenericContainer<RedpandaContainer> {
    public static final int REDPANDA_PORT = 9092;
    public static final int REDPANDA_ADMIN_PORT = 9644;
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    public static final int REST_PROXY_PORT = 8082;

    private boolean enableAuthorization;
    private String authenticationMethod = "none";
    private String schemaRegistryAuthenticationMethod = "none";

    private final List<String> superusers = new ArrayList<>();
    private final Set<Supplier<Listener>> listenersValueSupplier = new HashSet<>();

    public RedpandaContainer(DockerImageName imageName) {
        super(imageName);

        withExposedPorts(REDPANDA_PORT, REDPANDA_ADMIN_PORT, SCHEMA_REGISTRY_PORT, REST_PROXY_PORT);
        withCreateContainerCmdModifier(cmd -> {
            cmd.withEntrypoint();
            cmd.withUser("root:root");
        });
        waitingFor(Wait.forLogMessage(".*Successfully started Redpanda!.*", 1));
        withCopyFileToContainer(
                MountableFile.forClasspathResource("redpanda/entrypoint-tc.sh", 0700),
                "/entrypoint-tc.sh");
        withCommand("/entrypoint-tc.sh", "redpanda", "start", "--mode=dev-container", "--smp=1", "--memory=1G");
    }

    @Override
    protected void configure() {
        this.listenersValueSupplier.stream()
                .map(Supplier::get)
                .map(Listener::getAddress)
                .forEach(this::withNetworkAliases);
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        super.containerIsStarting(containerInfo);

        Configuration cfg = new Configuration(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
        cfg.setClassLoaderForTemplateLoading(getClass().getClassLoader(), "redpanda");
        cfg.setDefaultEncoding("UTF-8");

        try {
            var bf = getBootstrapFile(cfg);
            var rf = getRedpandaFile(cfg);

            LoggerFactory.getLogger(getClass()).debug("\n************************************************\n");
            LoggerFactory.getLogger(getClass()).debug(new String(bf.getBytes(), StandardCharsets.UTF_8));
            LoggerFactory.getLogger(getClass()).debug(new String(rf.getBytes(), StandardCharsets.UTF_8));
            LoggerFactory.getLogger(getClass()).debug("\n************************************************\n");

            copyFileToContainer(bf, "/etc/redpanda/.bootstrap.yaml");
            copyFileToContainer(rf, "/etc/redpanda/redpanda.yaml");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the bootstrap servers address.
     *
     * @return the bootstrap servers address
     */
    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getHost(), getMappedPort(REDPANDA_PORT));
    }

    /**
     * Returns the schema registry address.
     *
     * @return the schema registry address
     */
    public String getSchemaRegistryAddress() {
        return String.format("http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT));
    }

    /**
     * Returns the admin address.
     *
     * @return the admin address
     */
    public String getAdminAddress() {
        return String.format("http://%s:%s", getHost(), getMappedPort(REDPANDA_ADMIN_PORT));
    }

    /**
     * Returns the rest proxy address.
     *
     * @return the rest proxy address
     */
    public String getRestProxyAddress() {
        return String.format("http://%s:%s", getHost(), getMappedPort(REST_PROXY_PORT));
    }

    /**
     * Enables authorization.
     *
     * @return this {@link RedpandaContainer} instance
     */
    public RedpandaContainer enableAuthorization() {
        this.enableAuthorization = true;
        return this;
    }

    /**
     * Enables SASL.
     *
     * @return this {@link RedpandaContainer} instance
     */
    public RedpandaContainer enableSasl() {
        this.authenticationMethod = "sasl";
        return this;
    }

    /**
     * Enables Http Basic Auth for Schema Registry.
     *
     * @return this {@link RedpandaContainer} instance
     */
    public RedpandaContainer enableSchemaRegistryHttpBasicAuth() {
        this.schemaRegistryAuthenticationMethod = "http_basic";
        return this;
    }

    /**
     * Register username as a superuser.
     *
     * @param  username username to register as a superuser
     * @return          this {@link RedpandaContainer} instance
     */
    public RedpandaContainer withSuperuser(String username) {
        this.superusers.add(username);
        return this;
    }

    /**
     * Add a {@link Supplier} that will provide a listener with format {@code host:port}.
     * Host will be added as a network alias.
     * <p>
     * The listener will be added to the default listeners.
     * <p>
     * Default listeners:
     * <ul>
     * <li>0.0.0.0:9092</li>
     * <li>0.0.0.0:9093</li>
     * </ul>
     * <p>
     * Default advertised listeners:
     * <ul>
     * <li>{@code container.getHost():container.getMappedPort(9092)}</li>
     * <li>127.0.0.1:9093</li>
     * </ul>
     *
     * @param  listenerSupplier a supplier that will provide a listener
     * @return                  this {@link RedpandaContainer} instance
     */
    public RedpandaContainer withListener(Supplier<String> listenerSupplier) {
        String[] parts = listenerSupplier.get().split(":");
        this.listenersValueSupplier.add(() -> new Listener(parts[0], Integer.parseInt(parts[1])));
        return this;
    }

    private Transferable getBootstrapFile(Configuration cfg) throws Exception {
        Map<String, Object> kafkaApi = new HashMap<>();
        kafkaApi.put("enableAuthorization", this.enableAuthorization);
        kafkaApi.put("superusers", this.superusers);

        Map<String, Object> root = new HashMap<>();
        root.put("kafkaApi", kafkaApi);

        String file = resolveTemplate(cfg, "bootstrap.yaml.ftl", root);

        return Transferable.of(file, 0700);
    }

    private Transferable getRedpandaFile(Configuration cfg) throws Exception {
        List<Map<String, Object>> listeners = this.listenersValueSupplier.stream()
                .map(Supplier::get)
                .map(listener -> {
                    Map<String, Object> listenerMap = new HashMap<>();
                    listenerMap.put("address", listener.getAddress());
                    listenerMap.put("port", listener.getPort());
                    listenerMap.put("authentication_method", authenticationMethod);
                    return listenerMap;
                })
                .toList();

        Map<String, Object> kafkaApi = new HashMap<>();
        kafkaApi.put("authenticationMethod", this.authenticationMethod);
        kafkaApi.put("enableAuthorization", this.enableAuthorization);
        kafkaApi.put("advertisedHost", getHost());
        kafkaApi.put("advertisedPort", getMappedPort(9092));
        kafkaApi.put("listeners", listeners);

        Map<String, Object> schemaRegistry = new HashMap<>();
        schemaRegistry.put("authenticationMethod", this.schemaRegistryAuthenticationMethod);

        Map<String, Object> root = new HashMap<>();
        root.put("kafkaApi", kafkaApi);
        root.put("schemaRegistry", schemaRegistry);

        String file = resolveTemplate(cfg, "redpanda.yaml.ftl", root);

        return Transferable.of(file, 0700);
    }

    private String resolveTemplate(Configuration cfg, String template, Map<String, Object> data)
            throws IOException, TemplateException {
        Template temp = cfg.getTemplate(template);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Writer out = new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8);
        temp.process(data, out);

        return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
    }

    private static class Listener {
        private String address;
        private int port;

        public Listener(String address, int port) {
            this.address = address;
            this.port = port;
        }

        public String getAddress() {
            return address;
        }

        public Listener setAddress(String address) {
            this.address = address;
            return this;
        }

        public int getPort() {
            return port;
        }

        public Listener setPort(int port) {
            this.port = port;
            return this;
        }
    }
}
