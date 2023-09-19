package com.github.sco1237896.connector.core.deployment.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Set;
import java.util.TreeSet;

@JsonPropertyOrder({
        "catalog",
        "baseImage",
        "imageName",
        "types",
        "dependencies",
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectorImageDefinitionSpec {
    private final Set<String> dependencies;
    private final Set<String> types;

    private String imageName;
    private String baseImage;

    public ConnectorImageDefinitionSpec() {
        this.dependencies = new TreeSet<>();
        this.types = new TreeSet<>();
    }

    @JsonProperty
    public Set<String> getDependencies() {
        return dependencies;
    }

    @JsonProperty
    public Set<String> getTypes() {
        return types;
    }

    @JsonProperty
    public String getImageName() {
        return imageName;
    }

    @JsonProperty
    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    @JsonProperty
    public String getBaseImage() {
        return baseImage;
    }

    @JsonProperty
    public void setBaseImage(String baseImage) {
        this.baseImage = baseImage;
    }
}
