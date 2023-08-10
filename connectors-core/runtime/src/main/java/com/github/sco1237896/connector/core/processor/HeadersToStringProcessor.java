package com.github.sco1237896.connector.core.processor;

import org.apache.camel.Exchange;
import org.apache.camel.TypeConverter;

public class HeadersToStringProcessor {
    public void process(Exchange exchange, TypeConverter converter) throws Exception {
        exchange.getMessage().getHeaders().replaceAll(
                (k, v) -> {
                    if (k.startsWith("Camel")) {
                        return v;
                    }

                    return converter.convertTo(String.class, v);
                });

    }
}
