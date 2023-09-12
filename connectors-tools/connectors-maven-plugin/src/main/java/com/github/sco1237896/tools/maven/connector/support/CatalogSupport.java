package com.github.sco1237896.tools.maven.connector.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public final class CatalogSupport {
    public static final ObjectMapper YAML_MAPPER = new YAMLMapper()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
            .configure(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE, true)
            .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, false);

    public static final ObjectMapper JSON_MAPPER = JsonMapper.builder()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .build();

    private CatalogSupport() {
    }

    public static ClassLoader getClassLoader(MavenProject project) {
        try {
            List<String> classpathElements = project.getCompileClasspathElements();
            URL[] urls = new URL[classpathElements.size()];
            for (int i = 0; i < classpathElements.size(); ++i) {
                urls[i] = new File(classpathElements.get(i)).toURI().toURL();
            }
            return new URLClassLoader(urls, KameletsCatalog.class.getClassLoader());
        } catch (Exception e) {
            return KameletsCatalog.class.getClassLoader();
        }
    }

    public static String kameletType(ObjectNode node) {
        return node.requiredAt("/metadata/labels").get("camel.apache.org/kamelet.type").asText();
    }

    public static String kameletName(ObjectNode node) {
        return node.requiredAt("/metadata/name").asText();
    }

    public static String kameletVersion(ObjectNode node) {
        JsonNode annotations = node.requiredAt("/metadata/annotations");
        JsonNode version = annotations.get("camel.apache.org/kamelet.version");
        if (version == null) {
            version = annotations.get("camel.apache.org/catalog.version");
        }

        if (version == null) {
            return null;
        }

        return version.asText();
    }

    public static String asKey(String value) {
        String answer = CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE).convert(value);
        return CaseFormat.LOWER_HYPHEN.converterTo(CaseFormat.LOWER_UNDERSCORE).convert(answer);
    }

    public static List<Pair<String, String>> getCamelDescriptors(ArrayNode descriptors) {
        if (descriptors == null) {
            return Collections.emptyList();
        }

        List<Pair<String, String>> answer = new ArrayList<>();

        for (JsonNode node : descriptors) {
            String descriptor = node.asText();
            if (descriptor.startsWith("urn:camel:")) {
                descriptor = descriptor.substring("urn:camel:".length());

                String[] items = descriptor.split(":");
                if (items.length == 2) {
                    answer.add(Pair.of(items[0], items[1]));
                }
            }
        }

        return answer;
    }

    public static ObjectNode withProperty(JsonNode root, String propertyName, Consumer<ObjectNode> consumer) {
        ObjectNode answer = root.with("properties").with(propertyName);
        consumer.accept(answer);
        return answer;
    }

    public static ObjectNode withPropertyRef(JsonNode root, String group, String propertyName) {
        return withProperty(root, propertyName, d -> {
            d.put(
                    "$ref",
                    "#/$defs/" + group + "/" + propertyName);
        });
    }

    public static void disableAdditionalProperties(JsonNode root, String path) {
        JsonNode node = root.at(path);

        if (node.isMissingNode()) {
            return;
        }
        if (!node.isEmpty()) {
            return;
        }
        if (!node.isObject()) {
            return;
        }

        ((ObjectNode) node).put("type", "object");
        ((ObjectNode) node).put("additionalProperties", false);
    }
}
