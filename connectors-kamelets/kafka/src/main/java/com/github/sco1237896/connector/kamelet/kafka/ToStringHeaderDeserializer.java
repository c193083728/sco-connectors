package com.github.sco1237896.connector.kamelet.kafka;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.camel.component.kafka.serde.KafkaHeaderDeserializer;

// TODO: remove once https://issues.apache.org/jira/browse/CAMEL-19919 will be available
public class ToStringHeaderDeserializer implements KafkaHeaderDeserializer {
    private Charset charset = StandardCharsets.UTF_8;

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = Objects.requireNonNull(charset);
    }

    @Override
    public Object deserialize(String key, byte[] value) {
        return new String(value, this.charset);
    }
}
