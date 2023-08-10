package com.github.sco1237896.connector.core.processor;

import org.apache.camel.Exchange;
import org.apache.camel.TypeConverter;

public class SimulateErrorProcessor {
    public void process(Exchange exchange, TypeConverter converter) throws Exception {
        throw new RuntimeException("too bad");
    }
}
