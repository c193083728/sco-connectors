package com.github.sco1237896.tools.maven.connector.support;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConnectorDefinition {
    @Param(defaultValue = "${${connector.type}-${connector.version}")
    private String name;
    @Param(defaultValue = "${project.title}")
    private String title;
    @Param(defaultValue = "${project.description}")
    private String description;
    @Param(defaultValue = "${connector.version}")
    private String version;
    @Param(defaultValue = "${connector.annotations}")
    private Map<String, String> annotations = new TreeMap<>();

    @Param
    private EndpointRef source;
    @Param
    private EndpointRef sink;

    @Param
    private List<File> customizers;

    public List<File> getCustomizers() {
        return customizers;
    }

    public void setCustomizers(List<File> customizers) {
        this.customizers = customizers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public EndpointRef getSource() {
        return source;
    }

    public ConnectorDefinition setSource(EndpointRef source) {
        this.source = source;
        return this;
    }

    public EndpointRef getSink() {
        return sink;
    }

    public ConnectorDefinition setSink(EndpointRef sink) {
        this.sink = sink;
        return this;
    }

    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public ConnectorDefinition setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
        return this;
    }

    public static class EndpointRef {
        @Param
        String name;
        @Param
        String version;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }
}
