package com.github.sco1237896.connector.kamelet.serdes;

import org.apache.camel.Converter;

@Converter(generateLoader = true)
public class MimeTypeConverter {
    @Converter
    public static MimeType toMimeType(String type) {
        return MimeType.of(type);
    }
}
