package com.github.sco1237896.tools.maven.connector;

import io.quarkus.maven.BuildMojo;
import com.github.sco1237896.tools.maven.connector.support.CatalogSupport;
import com.github.sco1237896.tools.maven.connector.support.ConnectorIndex;
import com.github.sco1237896.tools.maven.connector.support.ConnectorManifest;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import com.github.sco1237896.tools.maven.connector.support.ConnectorDefinition;
import com.github.sco1237896.tools.maven.connector.support.MojoSupport;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builds the Quarkus application.
 */
@Mojo(name = "generate-app", defaultPhase = LifecyclePhase.PACKAGE, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME, threadSafe = true)
public class GenerateAppMojo extends BuildMojo {

    @Parameter(defaultValue = "false", property = "connectors.app.skip")
    private boolean skip = false;
    @Parameter(defaultValue = "false")
    private boolean exclude = false;

    @Parameter(defaultValue = "${project.artifactId}", property = "connector.type")
    private String type;
    @Parameter(defaultValue = "${project.version}", property = "connector.version")
    private String version;
    @Parameter(defaultValue = "0", property = "connector.initial-revision")
    private int initialRevision;
    @Parameter(defaultValue = "connector", property = "connector.container.image-prefix")
    private String containerImagePrefix;
    @Parameter(defaultValue = "${project.artifactId}", property = "connector.container.registry")
    private String containerImageRegistry;
    @Parameter(defaultValue = "${project.artifactId}", property = "connector.container.organization")
    private String containerImageOrg;
    @Parameter(property = "connector.container.additional-tags")
    private String containerImageAdditionalTags;
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

    @Parameter(defaultValue = "${camel-quarkus.version}")
    private String camelQuarkusVersion;

    @Parameter
    private ConnectorDefinition defaults;
    @Parameter
    private List<ConnectorDefinition> connectors;
    @Parameter
    private List<String> bannedDependencies;

    ConnectorManifest manifest;
    ConnectorManifest manifestLocal;
    String manifestId;
    Path manifestLocalFile;
    Path indexFile;
    ConnectorIndex index;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            getLog().info("Skipping App build");
            return;
        }

        try {
            this.manifestId = type.replace("-", "_");
            this.indexFile = indexPath.toPath().resolve("connectors.yaml");
            this.manifestLocalFile = definitionPathLocal.toPath().resolve(this.manifestId + ".yaml");

            if (!Files.exists(manifestLocalFile) && !exclude) {
                getLog().warn("Skipping App build as the definition file " + manifestLocalFile.getFileName() + " is missing");
                return;
            }

            this.index = MojoSupport.load(indexFile, ConnectorIndex.class, ConnectorIndex::new);
            this.manifestLocal = CatalogSupport.YAML_MAPPER.readValue(manifestLocalFile.toFile(), ConnectorManifest.class);
            this.manifest = index.getConnectors().get(this.manifestId);

            Files.createDirectories(definitionPath.toPath());

            if (!exclude) {
                if (manifest != null && manifest.getRevision() >= this.manifestLocal.getRevision()) {
                    getLog().info(
                            "Skipping App build (ref. revision:"
                                    + this.manifest.getRevision()
                                    + ", local revision: "
                                    + this.manifestLocal.getRevision()
                                    + ")");

                    return;
                }

                Set<String> propertiesToClear = new HashSet<>();
                propertiesToClear.add("quarkus.container-image.registry");
                propertiesToClear.add("quarkus.container-image.group");
                propertiesToClear.add("quarkus.container-image.name");
                propertiesToClear.add("quarkus.container-image.tag");
                propertiesToClear.add("quarkus.container-image.additional-tags");

                //
                // Sanitize system properties
                //

                if (mavenProject().getProperties() != null) {
                    for (String key : mavenProject().getProperties().stringPropertyNames()) {
                        if (propertiesToClear.contains(key)) {
                            getLog().warn("Removing project-property " + key);
                            mavenProject().getProperties().remove(key);
                        }
                    }
                }

                //
                // Set container image related properties
                //

                // TODO: this should be derived from the manifest
                System.setProperty("quarkus.container-image.registry", this.containerImageRegistry);
                System.setProperty("quarkus.container-image.group", this.containerImageOrg);
                System.setProperty("quarkus.container-image.name", this.containerImagePrefix + "-" + type);
                System.setProperty("quarkus.container-image.tag", this.containerImageTag);

                if (containerImageAdditionalTags != null) {
                    System.setProperty("quarkus.container-image.additional-tags", containerImageAdditionalTags);
                }

                getLog().info("App info:");

                for (String key : System.getProperties().stringPropertyNames()) {
                    if (key.startsWith("quarkus.container-image.")) {
                        getLog().info("  " + key + ": " + System.getProperties().getProperty(key));
                    }
                }

                super.execute();

                this.index.getConnectors().put(this.manifestId, this.manifestLocal);

                for (String type : this.manifestLocal.getTypes()) {
                    Path src = definitionPathLocal.toPath().resolve(type + ".yaml");
                    Path dst = definitionPath.toPath().resolve(type + ".yaml");

                    getLog().info("Copy connector definition " + src + " to " + dst);

                    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
                }
            } else {
                getLog().warn("Skipping App build as the connector is excluded");
                this.index.getConnectors().remove(this.manifestId);
            }

            getLog().info("Writing connector index to: " + this.indexFile);

            CatalogSupport.YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(
                    Files.newBufferedWriter(this.indexFile),
                    this.index);

            getLog().info("Cleaning up connectors");

            cleanup();

        } catch (Exception e) {
            throw new MojoExecutionException("Failed to build quarkus application", e);
        }
    }

    private void cleanup()
            throws MojoExecutionException, MojoFailureException {

        List<Path> definitions = new ArrayList<>();

        index.getConnectors().forEach((k, v) -> {
            for (String type : v.getTypes()) {
                definitions.add(indexPath.toPath().resolve(v.getCatalog()).resolve(type + ".yaml"));
            }
        });

        try (Stream<Path> files = Files.walk(indexPath.toPath())) {
            for (Path file : files.toList()) {
                if (!Files.isRegularFile(file)) {
                    continue;
                }
                if (file.getFileName().toString().equals("connectors.yaml")) {
                    continue;
                }

                if (!definitions.contains(file)) {
                    getLog().warn("Deleting " + file + " as it does not match any known connector in this module");
                    Files.delete(file);
                }
            }
        } catch (IOException e) {
            throw new MojoExecutionException(e);
        }
    }
}