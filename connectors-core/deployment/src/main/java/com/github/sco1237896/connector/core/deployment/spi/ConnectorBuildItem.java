package com.github.sco1237896.connector.core.deployment.spi;

import com.github.sco1237896.connector.core.deployment.model.ConnectorDefinition;
import com.github.sco1237896.connector.core.deployment.model.ConnectorImageDefinition;
import io.quarkus.builder.item.MultiBuildItem;

import java.util.List;

public final class ConnectorBuildItem extends MultiBuildItem {
    private final ConnectorImageDefinition imageDefinition;
    private final List<ConnectorDefinition> connectorDefinitions;
    private final boolean updateCatalog;

    public ConnectorBuildItem(ConnectorImageDefinition image, List<ConnectorDefinition> definitions, boolean updateCatalog) {
        this.imageDefinition = image;
        this.connectorDefinitions = definitions;
        this.updateCatalog = updateCatalog;
    }

    public ConnectorImageDefinition imageDefinition() {
        return imageDefinition;
    }

    public List<ConnectorDefinition> connectorDefinitions() {
        return connectorDefinitions;
    }

    public boolean updateCatalog() {
        return updateCatalog;
    }
}
