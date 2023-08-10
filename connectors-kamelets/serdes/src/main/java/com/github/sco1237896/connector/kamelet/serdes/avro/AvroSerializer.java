package com.github.sco1237896.connector.kamelet.serdes.avro;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;
import com.github.sco1237896.connector.kamelet.serdes.BaseSerializer;

import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.serde.avro.AvroSerdeHeaders;

public class AvroSerializer extends BaseSerializer<Schema> {
    private final AvroSerdeHeaders avroHeaders = new AvroSerdeHeaders(false);

    public AvroSerializer() {
        super(Avro.SCHEMA_PARSER);
    }

    @Override
    protected void configureHeaders(Headers headers) {
        avroHeaders.addEncodingHeader(headers, AvroEncoding.BINARY.name());
    }
}
