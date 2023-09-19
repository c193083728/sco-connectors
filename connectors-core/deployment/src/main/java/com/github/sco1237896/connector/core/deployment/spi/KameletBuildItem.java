package com.github.sco1237896.connector.core.deployment.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.builder.item.MultiBuildItem;

public final class KameletBuildItem extends MultiBuildItem {
    private final ObjectNode definition;

    public KameletBuildItem(ObjectNode definition) {
        this.definition = definition;
    }

    public ObjectNode getDefinition() {
        return definition;
    }
}
