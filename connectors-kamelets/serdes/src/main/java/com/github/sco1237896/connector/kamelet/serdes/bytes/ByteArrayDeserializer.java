package com.github.sco1237896.connector.kamelet.serdes.bytes;

import org.apache.kafka.common.header.Headers;

public class ByteArrayDeserializer extends org.apache.kafka.common.serialization.ByteArrayDeserializer {
    @Override
    public byte[] deserialize(String topic, Headers headers, byte[] data) {
        return super.deserialize(topic, headers, data);
    }
}
