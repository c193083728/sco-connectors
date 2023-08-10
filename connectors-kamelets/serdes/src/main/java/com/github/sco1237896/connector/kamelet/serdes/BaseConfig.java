package com.github.sco1237896.connector.kamelet.serdes;

import java.util.Map;

import io.apicurio.registry.serde.config.BaseKafkaSerDeConfig;

public class BaseConfig extends BaseKafkaSerDeConfig {
    public BaseConfig(Map<?, ?> originals) {
        super(originals);
    }
}
