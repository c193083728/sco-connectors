package com.github.sco1237896.tools.maven.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.sco1237896.tools.maven.connector.support.Annotation;
import com.github.sco1237896.tools.maven.connector.support.AppBootstrapProvider;
import com.github.sco1237896.tools.maven.connector.support.CatalogSupport;
import com.github.sco1237896.tools.maven.connector.support.ConnectorDefinition;
import com.github.sco1237896.tools.maven.connector.support.ConnectorIndex;
import com.github.sco1237896.tools.maven.connector.support.ConnectorManifest;
import com.github.sco1237896.tools.maven.connector.support.KameletsCatalog;
import com.github.sco1237896.tools.maven.connector.support.MojoSupport;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.quarkus.bootstrap.BootstrapException;
import io.quarkus.bootstrap.app.CuratedApplication;
import io.quarkus.maven.dependency.ResolvedDependency;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import net.javacrumbs.jsonunit.core.Option;
import org.apache.camel.v1.Kamelet;
import org.apache.camel.v1.Pipe;
import org.apache.camel.v1.PipeSpec;
import org.apache.camel.v1.pipespec.ErrorHandler;
import org.apache.camel.v1.pipespec.Sink;
import org.apache.camel.v1.pipespec.Source;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.plexus.component.annotations.Requirement;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.impl.RemoteRepositoryManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static com.github.sco1237896.tools.maven.connector.support.ConnectorConstants.CONNECTOR_REVISION;
import static com.github.sco1237896.tools.maven.connector.support.ConnectorConstants.TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE;
import static java.util.Optional.ofNullable;

@Mojo(name = "generate-catalog", defaultPhase = LifecyclePhase.PREPARE_PACKAGE, threadSafe = true, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME, requiresDependencyCollection = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class GenerateCatalogMojo extends AbstractMojo {

    @Parameter(defaultValue = "false", property = "connectors.catalog.skip")
    private boolean skip = false;

    @Parameter(defaultValue = "false", property = "connector.groups")
    private boolean groups = false;

    @Parameter(readonly = true, defaultValue = "${project}")
    private MavenProject project;

    @Parameter
    private List<Annotation> defaultAnnotations;
    @Parameter
    private List<Annotation> annotations;

    @Parameter(defaultValue = "${session}", readonly = true)
    protected MavenSession session;
    @Parameter
    private ConnectorDefinition defaults;
    @Parameter
    private List<ConnectorDefinition> connectors;

    @Parameter(defaultValue = "true", property = "connectors.catalog.validate")
    private boolean validate;

    @Parameter(defaultValue = "${project.artifactId}", property = "connector.type")
    private String type;
    @Parameter(defaultValue = "${project.version}", property = "connector.version")
    private String version;
    @Parameter(defaultValue = "0", property = "connector.initial-revision")
    private int initialRevision;
    @Parameter(defaultValue = "connector", property = "connector.container.image-prefix")
    private String containerImagePrefix;
    @Parameter(property = "connector.container.registry")
    private String containerImageRegistry;
    @Parameter(defaultValue = "${project.groupId}", property = "connector.container.organization")
    private String containerImageOrg;
    @Parameter(property = "connector.container.image.base")
    private String containerImageBase;
    @Parameter(property = "connector.container.tag", required = true)
    private String containerImageTag;

    @Parameter(defaultValue = "${project.build.outputDirectory}/META-INF/connectors")
    private File definitionPathLocal;
    @Parameter(defaultValue = "${connector.catalog.root}/${connectors.catalog.name}")
    private File definitionPath;
    @Parameter(defaultValue = "${connector.catalog.root}")
    private File indexPath;
    @Parameter(defaultValue = "${connectors.catalog.name}")
    private String catalogName;

    @Parameter(required = false, property = "appArtifact")
    private String appArtifact;
    @Parameter(defaultValue = "${project.build.directory}")
    protected File buildDir;
    @Parameter(defaultValue = "${project.build.finalName}")
    protected String finalName;
    @Parameter(defaultValue = "${camel-quarkus.version}")
    private String camelQuarkusVersion;
    @Requirement(role = RepositorySystem.class, optional = false)
    protected RepositorySystem repoSystem;
    @Requirement(role = RemoteRepositoryManager.class, optional = false)
    protected RemoteRepositoryManager remoteRepoManager;
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    private RepositorySystemSession repoSession;
    @Parameter
    private Map<String, String> systemProperties;

    @Parameter(defaultValue = "sco1237896.github.com", property = "annotation.prefix")
    private String annotationsPrefix;

    @Component
    protected MavenProjectHelper projectHelper;

    ConnectorManifest manifest;
    String manifestId;
    Path manifestFile;
    Path manifestLocalFile;
    Path indexFile;
    ConnectorIndex index;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            getLog().info("Skipping generate-catalog");
        }

        try {
            this.manifestId = type.replace("-", "_");
            this.indexFile = indexPath.toPath().resolve("connectors.yaml");
            this.manifestFile = definitionPath.toPath().resolve(this.manifestId + ".yaml");
            this.manifestLocalFile = definitionPathLocal.toPath().resolve(this.manifestId + ".yaml");
            this.index = MojoSupport.load(indexFile, ConnectorIndex.class, ConnectorIndex::new);

            this.manifest = index.getConnectors().computeIfAbsent(this.manifestId, k -> {
                return new ConnectorManifest(
                        this.catalogName,
                        this.initialRevision,
                        Collections.emptySet(),
                        null,
                        this.containerImageBase,
                        null);
            });

            final KameletsCatalog kameletsCatalog = KameletsCatalog.get(project, getLog());
            final List<ConnectorDefinition> connectorList = MojoSupport.inject(session, defaults, connectors);

            //
            // Update manifest dependencies
            //

            TreeSet<String> newDependencies = new TreeSet<>(dependencies());

            if (!this.manifest.getDependencies().equals(newDependencies)) {
                SetUtils.SetView<String> diff = SetUtils.difference(this.manifest.getDependencies(), newDependencies);
                if (diff.isEmpty()) {
                    diff = SetUtils.difference(newDependencies, this.manifest.getDependencies());
                }

                if (!diff.isEmpty()) {
                    getLog().info("Detected diff in dependencies (" + diff.size() + "):");
                    diff.forEach(d -> {
                        getLog().info("  " + d);
                    });
                } else {
                    getLog().info("Detected diff in dependencies (" + diff.size() + ")");
                }

                this.manifest.bump();
                this.manifest.getDependencies().clear();
                this.manifest.getDependencies().addAll(newDependencies);
            }

            if (!Objects.equals(manifest.getBaseImage(), this.containerImageBase)) {
                getLog().info("Detected diff in base image");

                this.manifest.setBaseImage(this.containerImageBase);
                this.manifest.bump();
            }

            //
            // Connectors
            //

            for (ConnectorDefinition connector : connectorList) {
                Pipe def = generateDefinitions(kameletsCatalog, connector);

                this.manifest.getTypes().add(def.getMetadata().getAnnotations().get("sco1237896.github.com/connector.id"));
            }

            //
            // Manifest
            //

            getLog().info("Writing connector manifest to: " + manifestLocalFile);

            this.manifest.setImage(
                    String.format("%s/%s/%s-%s:%s",
                            this.containerImageRegistry,
                            this.containerImageOrg,
                            this.containerImagePrefix, this.type,
                            this.containerImageTag));

            CatalogSupport.YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(
                    Files.newBufferedWriter(manifestLocalFile),
                    this.manifest);

        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }
    }

    private void cleanupKamelet(ObjectNode kamelet) {
        JsonNode meta = kamelet.at("/metadata");
        if (meta.isObject()) {
            ((ObjectNode) meta).remove("annotations");
            ((ObjectNode) meta).remove("labels");
        }

        JsonNode spec = kamelet.at("/spec");
        if (spec.isObject()) {
            ((ObjectNode) spec).remove("template");
            ((ObjectNode) spec).remove("dependencies");
        }
    }

    private Pipe generateDefinitions(KameletsCatalog kamelets, ConnectorDefinition definition)
            throws MojoExecutionException, MojoFailureException {

        try {
            final String version = ofNullable(definition.getVersion()).orElseGet(project::getVersion);
            final String name = ofNullable(definition.getName()).orElseGet(project::getArtifactId);
            final String title = ofNullable(definition.getTitle()).orElseGet(project::getName);
            final String description = ofNullable(definition.getDescription()).orElseGet(project::getDescription);
            final String id = name.replace("-", "_");

            final Path definitionFile = definitionPath.toPath().resolve(id + ".yaml");
            final Path definitionLocalFile = definitionPathLocal.toPath().resolve(id + ".yaml");

            final ConnectorDefinition.EndpointRef source = definition.getSource();
            final ConnectorDefinition.EndpointRef sink = definition.getSink();

            ObjectNode sourceKamelet = kamelets.kamelet(source.getName(), source.getVersion());
            cleanupKamelet(sourceKamelet);

            ObjectNode sinkKamelet = kamelets.kamelet(sink.getName(), sink.getVersion());
            cleanupKamelet(sinkKamelet);

            final Pipe pipe = new Pipe();
            pipe.setMetadata(new ObjectMeta());
            pipe.getMetadata().setAnnotations(new TreeMap<>());
            pipe.setSpec(new PipeSpec());

            var srcRef = new org.apache.camel.v1.pipespec.source.Ref();
            srcRef.setApiVersion(HasMetadata.getApiVersion(Kamelet.class));
            srcRef.setKind(HasMetadata.getKind(Kamelet.class));
            srcRef.setName(source.getName());

            Source src = new Source();
            src.setRef(srcRef);

            pipe.getSpec().setSource(src);

            var snkRef = new org.apache.camel.v1.pipespec.sink.Ref();
            snkRef.setApiVersion(HasMetadata.getApiVersion(Kamelet.class));
            snkRef.setKind(HasMetadata.getKind(Kamelet.class));
            snkRef.setName(sink.getName());

            Sink snk = new Sink();
            snk.setRef(snkRef);

            pipe.getSpec().setSink(snk);

            pipe.getMetadata().getAnnotations().put(annotationsPrefix + "/resource.type", "connector");
            pipe.getMetadata().getAnnotations().put(annotationsPrefix + "/connector.group", catalogName);
            pipe.getMetadata().getAnnotations().put(annotationsPrefix + "/connector.id", id);
            pipe.getMetadata().getAnnotations().put(annotationsPrefix + "/connector.title", title);
            pipe.getMetadata().getAnnotations().put(annotationsPrefix + "/connector.description", description);
            pipe.getMetadata().getAnnotations().put(annotationsPrefix + "/connector.version", version);

            ErrorHandler eh = new ErrorHandler();
            eh.setAdditionalProperty("log", Map.of());

            pipe.getSpec().setErrorHandler(eh);

            definition.getAnnotations().forEach((k, v) -> {
                pipe.getMetadata().getAnnotations().putIfAbsent(annotationsPrefix + "/" + k, v);
            });

            //
            // Patch
            //

            if (definition.getCustomizers() != null) {
                ImportCustomizer ic = new ImportCustomizer();

                CompilerConfiguration cc = new CompilerConfiguration();
                cc.addCompilationCustomizers(ic);

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                Binding binding = new Binding();
                binding.setProperty("mapper", CatalogSupport.YAML_MAPPER);
                binding.setProperty("log", getLog());
                binding.setProperty("connector", definition);
                binding.setProperty("pipe", pipe);
                binding.setProperty("pipe", pipe);
                binding.setProperty("pipe", pipe);

                for (File customizer : definition.getCustomizers()) {
                    if (!Files.exists(customizer.toPath())) {
                        continue;
                    }

                    getLog().info("Customizing: " + definition.getName() + " with customizer " + customizer);

                    new GroovyShell(cl, binding, cc).run(customizer, new String[] {});
                }
            }

            //
            // Revision
            //

            Map<String, Object> def = new TreeMap<>();
            def.put("definition", pipe);
            def.put("resources", List.of(sourceKamelet, sinkKamelet));

            try {
                if (Files.exists(definitionFile)) {
                    JsonNode newSchema = CatalogSupport.YAML_MAPPER.convertValue(def, ObjectNode.class);
                    JsonNode oldSchema = CatalogSupport.YAML_MAPPER.readValue(definitionFile.toFile(), JsonNode.class);

                    JsonNode annotations = oldSchema.at("/definition/metadata/annotations");
                    if (!annotations.isMissingNode() && annotations.isObject()) {
                        ((ObjectNode) annotations).remove(TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE);
                        ((ObjectNode) annotations).remove(annotationsPrefix + "/connector.revision");
                    }

                    JsonAssertions.assertThatJson(oldSchema)
                            // for some reason this does not seem to be working even if the json path is correct
                            //
                            //.whenIgnoringPaths(
                            //    "$.definition.metadata.annotations['" + TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE + "']",
                            //    "$.definition.metadata.annotations['" + CONNECTOR_REVISION + "']")
                            .withDifferenceListener((difference, context) -> {
                                getLog().info("diff: " + difference);
                                manifest.bump();
                            })
                            .when(Option.IGNORING_ARRAY_ORDER)
                            .isEqualTo(newSchema);
                }
            } catch (AssertionError e) {
                // ignored, just avoid blowing thing up
            }

            //
            // Images
            //

            String image = String.format("%s/%s/%s-%s:%s",
                    this.containerImageRegistry,
                    this.containerImageOrg,
                    this.containerImagePrefix, this.type,
                    this.containerImageTag);

            pipe.getMetadata().getAnnotations().put(
                    TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE,
                    image);
            pipe.getMetadata().getAnnotations().put(
                    annotationsPrefix + "/connector.revision",
                    Integer.toString(this.manifest.getRevision()));

            //
            // Write Definition
            //

            Files.createDirectories(definitionPathLocal.toPath());

            getLog().info("Writing connector definition to: " + definitionLocalFile);

            CatalogSupport.YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(
                    Files.newBufferedWriter(definitionLocalFile),
                    def);

            return pipe;

        } catch (IOException e) {
            throw new MojoExecutionException("", e);
        }
    }

    public TreeSet<String> dependencies()
            throws MojoExecutionException, MojoFailureException {

        TreeSet<String> answer = new TreeSet<>();

        try {
            Set<String> propertiesToClear = new HashSet<>();
            propertiesToClear.add("quarkus.container-image.build");
            propertiesToClear.add("quarkus.container-image.push");

            // disable quarkus build
            System.setProperty("quarkus.container-image.build", "false");
            System.setProperty("quarkus.container-image.push", "false");

            if (systemProperties != null) {
                // Add the system properties of the plugin to the system properties
                // if and only if they are not already set.
                for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
                    String key = entry.getKey();
                    if (System.getProperty(key) == null) {
                        System.setProperty(key, entry.getValue());
                        propertiesToClear.add(key);
                    }
                }
            }

            try (CuratedApplication curatedApplication = bootstrapApplication().bootstrapQuarkus().bootstrap()) {
                List<ResolvedDependency> deps = new ArrayList<>(curatedApplication.getApplicationModel().getDependencies());
                deps.sort(Comparator.comparing(ResolvedDependency::toCompactCoords));

                for (ResolvedDependency dep : deps) {
                    MessageDigest digest = DigestUtils.getSha256Digest();
                    Path path = dep.getResolvedPaths().getSinglePath();

                    if (dep.getGroupId().startsWith("com.github.sco1237896")) {
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
                                if (entry.getName().endsWith(".kamelet.yaml")) {
                                    DigestUtils.updateDigest(digest, entry.getName());
                                }

                                try (InputStream is = jar.getInputStream(entry)) {
                                    DigestUtils.updateDigest(digest, is);
                                }
                            }
                        }
                    } else {
                        DigestUtils.updateDigest(digest, dep.toCompactCoords());
                    }

                    answer.add(
                            dep.toCompactCoords() + "@sha256:" + DigestUtils.sha256Hex(digest.digest()));
                }
            } finally {
                // Clear all the system properties set by the plugin
                propertiesToClear.forEach(System::clearProperty);
            }
        } catch (BootstrapException | IOException e) {
            throw new MojoExecutionException("Failed to build quarkus application", e);
        }

        return answer;
    }

    protected AppBootstrapProvider bootstrapApplication() {
        AppBootstrapProvider provider = new AppBootstrapProvider();
        provider.setAppArtifactCoords(this.appArtifact);
        provider.setBuildDir(this.buildDir);
        provider.setConnectors(this.connectors);
        provider.setDefaults(this.defaults);
        provider.setFinalName(this.finalName);
        provider.setLog(getLog());
        provider.setProject(this.project);
        provider.setCamelQuarkusVersion(this.camelQuarkusVersion);
        provider.setRemoteRepoManager(this.remoteRepoManager);
        provider.setRepoSession(this.repoSession);
        provider.setRepoSystem(this.repoSystem);
        provider.setSession(this.session);

        return provider;
    }
}
