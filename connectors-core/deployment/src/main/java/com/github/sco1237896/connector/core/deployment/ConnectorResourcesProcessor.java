package com.github.sco1237896.connector.core.deployment;

import com.github.sco1237896.connector.core.deployment.model.ConnectorDefinition;
import com.github.sco1237896.connector.core.deployment.spi.ConnectorBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.KameletBuildItem;
import com.github.sco1237896.connector.core.deployment.support.ConnectorSupport;
import io.quarkus.container.spi.ContainerImageBuildRequestBuildItem;
import io.quarkus.container.spi.ContainerImageCustomNameBuildItem;
import io.quarkus.deployment.ApplicationArchive;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationArchivesBuildItem;
import io.quarkus.deployment.builditem.GeneratedFileSystemResourceBuildItem;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class ConnectorResourcesProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorResourcesProcessor.class);

    @BuildStep
    void kamelets(
            ConnectorConfiguration configuration,
            ApplicationArchivesBuildItem archives,
            BuildProducer<KameletBuildItem> kamelets) {

        for (ApplicationArchive archive : archives.getAllApplicationArchives()) {
            for (Path root : archive.getRootDirectories()) {
                final Path resourcePath = root.resolve(ConnectorSupport.KAMELETS_BASE_PATH);

                if (!Files.exists(resourcePath)) {
                    continue;
                }
                if (!Files.isDirectory(resourcePath)) {
                    continue;
                }

                try (Stream<Path> files = Files.list(resourcePath)) {
                    files.filter(Files::isRegularFile)
                            .filter(f -> f.getFileName().toString().endsWith(ConnectorSupport.KAMELETS_EXTENSION))
                            .map(ConnectorSupport::readValue)
                            .map(KameletBuildItem::new)
                            .forEach(kamelets::produce);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @BuildStep
    void generateResources(
            ConnectorConfiguration configuration,
            List<ConnectorBuildItem> connectors,
            BuildProducer<GeneratedResourceBuildItem> resources,
            BuildProducer<GeneratedFileSystemResourceBuildItem> fileSystemResources,
            BuildProducer<ContainerImageBuildRequestBuildItem> containerImageRequest,
            BuildProducer<ContainerImageCustomNameBuildItem> containerImageName)
            throws Exception {

        Path catalogRoot = Path.of(configuration.catalog().root());
        Path catalogGroupRoot = catalogRoot.resolve("connector-catalog-" + configuration.catalog().group());

        for (ConnectorBuildItem connector : connectors) {

            for (ConnectorDefinition definition : connector.connectorDefinitions()) {
                byte[] content = ConnectorSupport.WRITER.writeValueAsBytes(definition);
                String id = ConnectorSupport.getConnectorId(definition.getSpec().getDefinition());

                String localPath = "META-INF/connectors/" + id + ".yaml";
                String catalogPath = catalogGroupRoot.resolve(id + ".yaml").toString();

                LOGGER.info("connector definition (local) {}", localPath);
                LOGGER.info("connector definition (catalog) {}", catalogPath);

                resources.produce(
                        new GeneratedResourceBuildItem(localPath, content));
                fileSystemResources.produce(
                        new GeneratedFileSystemResourceBuildItem(catalogPath, content));
            }

            if (connector.updateCatalog()) {
                byte[] content = ConnectorSupport.WRITER.writeValueAsBytes(connector.imageDefinition());
                String id = ConnectorSupport.getCatalogId(connector.imageDefinition());

                String localPath = "META-INF/connectors/" + id + ".yaml";
                String catalogPath = catalogGroupRoot.resolve(id + ".yaml").toString();

                LOGGER.info("connector image definition (local) {}", localPath);
                LOGGER.info("connector image definition (catalog) {}", catalogPath);

                resources.produce(
                        new GeneratedResourceBuildItem(localPath, content));
                fileSystemResources.produce(
                        new GeneratedFileSystemResourceBuildItem(catalogPath, content));

                containerImageRequest.produce(
                        new ContainerImageBuildRequestBuildItem());
                containerImageName
                        .produce(new ContainerImageCustomNameBuildItem(connector.imageDefinition().getSpec().getImageName()));
            }
        }
    }
}
