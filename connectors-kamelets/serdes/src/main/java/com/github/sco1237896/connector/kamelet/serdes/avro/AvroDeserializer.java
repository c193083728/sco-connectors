package com.github.sco1237896.connector.kamelet.serdes.avro;

import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;
import com.github.sco1237896.connector.kamelet.serdes.BaseDeserializer;
import com.github.sco1237896.connector.kamelet.serdes.Serdes;

import io.apicurio.registry.resolver.ParsedSchema;

public class AvroDeserializer extends BaseDeserializer<Schema> {
    public AvroDeserializer() {
        super(Avro.SCHEMA_PARSER);
    }

    @Override
    protected void configureHeaders(Headers headers, ParsedSchema<Schema> schema) {
        headers.add(
                Serdes.CONTENT_SCHEMA,
                schema.getParsedSchema().toString().getBytes(StandardCharsets.UTF_8));
    }
}
