package com.github.sco1237896.connector.core.deployment.spi;

import com.github.sco1237896.connector.core.deployment.model.ConnectorImageDefinition;
import io.quarkus.builder.item.MultiBuildItem;

public final class ConnectorImageDefinitionBuildItem extends MultiBuildItem {
    private final ConnectorImageDefinition image;

    public ConnectorImageDefinitionBuildItem(ConnectorImageDefinition image) {
        this.image = image;
    }

    public ConnectorImageDefinition image() {
        return this.image;
    }
}
