package com.github.sco1237896.connector.core.deployment.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.v1.Pipe;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder({
        "definition",
        "resources",
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectorDefinitionSpec {
    private final List<ObjectNode> resources;

    private Pipe definition;

    public ConnectorDefinitionSpec() {
        this.resources = new ArrayList<>();
    }

    @JsonProperty
    public Pipe getDefinition() {
        return definition;
    }

    @JsonProperty
    public void setDefinition(Pipe definition) {
        this.definition = definition;
    }

    @JsonProperty
    public List<ObjectNode> getResources() {
        return resources;
    }
}
