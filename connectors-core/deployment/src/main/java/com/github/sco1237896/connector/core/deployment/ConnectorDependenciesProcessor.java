package com.github.sco1237896.connector.core.deployment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.zerodep.shaded.org.apache.commons.codec.digest.DigestUtils;
import com.github.sco1237896.connector.core.deployment.model.ConnectorDependency;
import com.github.sco1237896.connector.core.deployment.spi.KameletBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.PomEnricherBuildItem;
import com.github.sco1237896.connector.core.deployment.support.Builders;
import com.github.sco1237896.connector.core.deployment.support.ConnectorSupport;
import io.quarkus.bootstrap.model.ApplicationModel;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Produce;
import io.quarkus.deployment.builditem.ApplicationArchivesBuildItem;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import org.l2x6.pom.tuner.Comparators;
import org.l2x6.pom.tuner.model.Gavtcs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ConnectorDependenciesProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorDependenciesProcessor.class);

    @Produce(PomEnricherBuildItem.class)
    @BuildStep
    void enrichDependencies(
            ConnectorConfiguration configuration,
            ApplicationArchivesBuildItem archive,
            CurateOutcomeBuildItem outcome,
            List<KameletBuildItem> kamelets)
            throws Exception {

        String oldDigest = pomChecksum(outcome.getApplicationModel());
        enrichPom(configuration, outcome.getApplicationModel(), kamelets);
        String newDigest = pomChecksum(outcome.getApplicationModel());

        if (!Objects.equals(oldDigest, newDigest)) {
            throw new RuntimeException(
                    "The dependencies have changed and the pom.xml has been overwritten, please rebuild");
        }
    }

    private String pomChecksum(ApplicationModel app) throws IOException {

        Path moduleDir = app.getAppArtifact().getWorkspaceModule().getModuleDir().toPath();
        Path pom = moduleDir.resolve("pom.xml");

        try (InputStream is = Files.newInputStream(pom)) {
            return DigestUtils.sha256Hex(is);
        }
    }

    private void enrichPom(
            ConnectorConfiguration configuration,
            ApplicationModel app,
            List<KameletBuildItem> kamelets) throws Exception {

        Path moduleDir = app.getApplicationModule().getModuleDir().toPath();
        Path pom = moduleDir.resolve("pom.xml");

        var builder = new Builders.Pom();
        builder = builder.withPath(pom);

        builder = builder.withTransformation((document, context) -> {
            var profile = context.getOrAddProfile("kamelets-deps");
            profile.prependCommentIfNeeded("This is auto generate, do not change it");
            profile.getOrAddChildContainerElement("activation").addChildTextElementIfNeeded(
                    "activeByDefault",
                    "true",
                    Comparators.entryValueOnly());

            var deps = profile.getOrAddChildContainerElement("dependencies");

            for (var child : deps.childElements()) {
                child.remove(true, true);
            }

            for (var dep : dependencies(configuration, kamelets)) {
                deps.addGavtcsIfNeeded(
                        new Gavtcs(dep.groupId, dep.artifactId, null),
                        Gavtcs.groupFirstComparator());
            }
        });

        LOGGER.info("Writing pom.xml");

        builder.build();

    }

    public static Collection<ConnectorDependency> dependencies(
            ConnectorConfiguration configuration,
            List<KameletBuildItem> kamelets) {

        final Set<ConnectorDependency> dependencies = new HashSet<>();
        final List<ObjectNode> requiredKamelets = new ArrayList<>();

        for (ConnectorConfiguration.ConnectorDefinition connector : configuration.connector().definitions()) {
            ConnectorSupport.lookupKamelet(connector.source(), kamelets)
                    .ifPresent(requiredKamelets::add);
            ConnectorSupport.lookupKamelet(connector.sink(), kamelets)
                    .ifPresent(requiredKamelets::add);
        }

        for (ObjectNode node : requiredKamelets) {
            LOGGER.info("kamelet: " + node.at("/metadata/name").asText());

            for (JsonNode depNode : node.at("/spec/dependencies")) {

                final String dep = depNode.asText();
                final ConnectorDependency result;

                if (dep.startsWith("mvn:")) {
                    String[] coords = dep.substring("mvn:".length()).split(":");

                    if (Objects.equals(coords[0], "org.apache.camel.kamelets")
                            && Objects.equals(coords[1], "camel-kamelets-utils")) {
                        continue;
                    }

                    LOGGER.info(">> " + node.at("/metadata/name").asText());

                    result = new ConnectorDependency(
                            coords[0],
                            coords[1],
                            coords[2]);

                } else if (dep.startsWith("camel:")) {
                    String coord = dep.substring("camel:".length());
                    result = new ConnectorDependency(
                            "org.apache.camel.quarkus",
                            "camel-quarkus-" + coord);

                } else if (dep.startsWith("github:")) {
                    String[] coords = dep.substring("github:".length()).split(":");
                    result = new ConnectorDependency(
                            "com.github." + coords[0],
                            coords[1],
                            coords[2]);
                } else {
                    throw new RuntimeException("Unsupported dependency: " + dep);
                }

                LOGGER.info(">> " + result);

                dependencies.add(result);

            }
        }

        return dependencies;
    }
}
