package com.github.sco1237896.connector.core.deployment;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.sco1237896.connector.core.deployment.model.ConnectorDefinition;
import com.github.sco1237896.connector.core.deployment.model.ConnectorImageDefinition;
import com.github.sco1237896.connector.core.deployment.spi.ConnectorDefinitionBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.ConnectorImageDefinitionBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.ConnectorBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.PomEnricherBuildItem;
import com.github.sco1237896.connector.core.deployment.support.ConnectorSupport;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import io.quarkus.maven.dependency.ResolvedDependency;
import net.javacrumbs.jsonunit.core.Option;
import net.javacrumbs.jsonunit.core.internal.Diff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectorImageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorImageProcessor.class);

    /**
     *
     * @param configuration the connector configuration
     * @param definitions   the connectors definitions
     * @param images        the images definitions
     * @param connectors    the connectors to generate
     */
    @Consume(PomEnricherBuildItem.class)
    @BuildStep
    void generateConnectors(
            ConnectorConfiguration configuration,
            List<ConnectorDefinitionBuildItem> definitions,
            List<ConnectorImageDefinitionBuildItem> images,
            BuildProducer<ConnectorBuildItem> connectors)
            throws Exception {

        String selectedImageName = String.format("%s-%s",
                configuration.container().imagePrefix(),
                configuration.connector().type());

        String selectedImage = String.format("%s/%s/%s:%s",
                configuration.container().registry(),
                configuration.container().organization(),
                selectedImageName,
                configuration.container().tag());

        for (ConnectorImageDefinitionBuildItem item : images) {

            ConnectorImageDefinition image = item.image();
            String id = ConnectorSupport.getCatalogId(item.image().getMetadata());

            Path catalogRoot = Path.of(configuration.catalog().root());
            Path catalogGroupRoot = catalogRoot.resolve(configuration.catalog().name());
            Path catalogManifest = catalogGroupRoot.resolve(id + ".yaml");

            final AtomicBoolean buildImage = new AtomicBoolean(false);

            if (Files.isRegularFile(catalogManifest) && Files.exists(catalogManifest)) {
                try (InputStream manifestIs = Files.newInputStream(catalogManifest)) {
                    ObjectNode oldSchema = ConnectorSupport.MAPPER.readValue(manifestIs, ObjectNode.class);
                    ObjectNode newSchema = ConnectorSupport.MAPPER.convertValue(image, ObjectNode.class);
                    ObjectNode refSchema = oldSchema.deepCopy();

                    ConnectorSupport.removeFields(oldSchema, "/metadata", List.of("name"));
                    ConnectorSupport.removeFields(oldSchema, "/spec", List.of("imageName"));
                    ConnectorSupport.removeFields(newSchema, "/metadata", List.of("name"));
                    ConnectorSupport.removeFields(newSchema, "/spec", List.of("imageName"));

                    Diff schemaDiff = ConnectorSupport.diff(
                            newSchema,
                            oldSchema,
                            conf -> {
                                return conf
                                        .when(Option.IGNORING_ARRAY_ORDER)
                                        .withDifferenceListener((d, c) -> LOGGER.info("diff: {}", d));
                            });

                    if (!schemaDiff.similar()) {
                        buildImage.set(true);
                    }

                    for (String type : image.getSpec().getTypes()) {
                        ConnectorDefinitionBuildItem definition = definitions.stream()
                                .filter(d -> {
                                    return ConnectorSupport.hasConnectorId(d.definition().getSpec().getDefinition(), type);
                                }).findFirst().orElseThrow(() -> {
                                    return new RuntimeException("Unable to find connector id " + type);
                                });

                        try (InputStream connectorIs = Files.newInputStream(catalogGroupRoot.resolve(type + ".yaml"))) {

                            ObjectNode oldDef = ConnectorSupport.MAPPER.readValue(connectorIs, ObjectNode.class);
                            ObjectNode newDef = ConnectorSupport.MAPPER.convertValue(definition.definition(), ObjectNode.class);

                            ConnectorSupport.removeFields(oldDef, "/spec/definition/metadata/annotations", List.of(
                                    ConnectorSupport.TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE,
                                    ConnectorSupport.API_GROUP + "/connector.revision"));
                            ConnectorSupport.removeFields(newDef, "/spec/definition/metadata/annotations", List.of(
                                    ConnectorSupport.TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE,
                                    ConnectorSupport.API_GROUP + "/connector.revision"));

                            Diff defDiff = ConnectorSupport.diff(
                                    newDef,
                                    oldDef,
                                    conf -> {
                                        // for some reason this does not seem to be working even if the json path is correct
                                        //
                                        //.whenIgnoringPaths(
                                        //    "$.spec.definition.metadata.annotations['" + TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE + "']",
                                        //    "$.spec.definition.metadata.annotations['" + CONNECTOR_REVISION + "']")
                                        return conf
                                                .when(Option.IGNORING_ARRAY_ORDER)
                                                .withDifferenceListener((d, c) -> LOGGER.info("diff: {}", d));
                                    });

                            if (!defDiff.similar()) {
                                buildImage.set(true);
                            }
                        }
                    }

                    if (!buildImage.get()) {
                        selectedImageName = refSchema.requiredAt("/spec/imageName").textValue();
                        selectedImage = refSchema.requiredAt("/metadata/name").textValue();
                    }
                }
            } else {
                buildImage.set(true);
            }

            image.getSpec().setImageName(selectedImageName);
            image.getMetadata().setName(selectedImage);

            ConnectorBuildItem cbi = new ConnectorBuildItem(image, new ArrayList<>(), buildImage.get());

            for (String type : image.getSpec().getTypes()) {
                ConnectorDefinitionBuildItem definition = definitions.stream()
                        .filter(d -> {
                            return ConnectorSupport.hasConnectorId(d.definition().getSpec().getDefinition(), type);
                        }).findFirst().orElseThrow(() -> {
                            return new RuntimeException("Unable to find connector id " + type);
                        });

                // clone to safety amend it
                ObjectNode oldDef = ConnectorSupport.MAPPER.convertValue(definition.definition(), ObjectNode.class);
                ConnectorDefinition newDef = ConnectorSupport.MAPPER.convertValue(oldDef, ConnectorDefinition.class);

                newDef.getSpec().getDefinition().getMetadata().getAnnotations().put(
                        ConnectorSupport.TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE,
                        selectedImage);

                cbi.connectorDefinitions().add(newDef);
            }

            connectors.produce(cbi);

        }
    }

    @BuildStep
    void generateImageDefinitions(
            ConnectorConfiguration configuration,
            CurateOutcomeBuildItem outcome,
            List<ConnectorDefinitionBuildItem> definitions,
            BuildProducer<ConnectorImageDefinitionBuildItem> resources) {

        ResolvedDependency appArtifactId = outcome.getApplicationModel().getAppArtifact();

        ConnectorImageDefinition image = new ConnectorImageDefinition();
        image.getMetadata().setAnnotations(Map.of(
                ConnectorSupport.API_GROUP + "/catalog.id", configuration.connector().type().replace("-", "_"),
                ConnectorSupport.API_GROUP + "/catalog.name", configuration.connector().type(),
                ConnectorSupport.API_GROUP + "/catalog.group", configuration.catalog().group()));

        image.getSpec().setBaseImage(configuration.container().imageBae());

        outcome.getApplicationModel().getDependencies().stream()
                .sorted(Comparator.comparing(o -> o.getKey().toGacString()))
                .filter(rd -> !ConnectorSupport.equals(rd, appArtifactId))
                .map(rd -> rd.getKey().toGacString() + "@sha256:" + ConnectorSupport.computeSha256Digest(rd))
                .forEach(s -> image.getSpec().getDependencies().add(s));

        for (ConnectorDefinitionBuildItem item : definitions) {
            image.getSpec().getTypes()
                    .add(item.definition().getSpec().getDefinition().getMetadata().getAnnotations()
                            .get(ConnectorSupport.API_GROUP + "/connector.id"));
        }

        resources.produce(new ConnectorImageDefinitionBuildItem(image));

    }
}
