package com.github.sco1237896.connector.kamelet.serdes.json;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;
import com.github.sco1237896.connector.kamelet.serdes.BaseDeserializer;
import com.github.sco1237896.connector.kamelet.serdes.Serdes;

import com.fasterxml.jackson.databind.JsonNode;

import io.apicurio.registry.resolver.ParsedSchema;

public class JsonDeserializer extends BaseDeserializer<JsonNode> {
    public JsonDeserializer() {
        super(Json.SCHEMA_PARSER);
    }

    @Override
    protected void configureHeaders(Headers headers, ParsedSchema<JsonNode> schema) {
        headers.add(
                Serdes.CONTENT_SCHEMA,
                schema.getParsedSchema().toString().getBytes(StandardCharsets.UTF_8));
    }
}
