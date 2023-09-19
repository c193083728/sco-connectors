package com.github.sco1237896.connector.core.deployment.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.sco1237896.connector.core.deployment.model.ConnectorDefinition;
import io.quarkus.builder.item.MultiBuildItem;
import org.apache.camel.v1.Pipe;

import java.util.List;

public final class ConnectorDefinitionBuildItem extends MultiBuildItem {
    private final ConnectorDefinition definition;

    public ConnectorDefinitionBuildItem(ConnectorDefinition definition) {
        this.definition = definition;
    }

    public ConnectorDefinition definition() {
        return definition;
    }
}
