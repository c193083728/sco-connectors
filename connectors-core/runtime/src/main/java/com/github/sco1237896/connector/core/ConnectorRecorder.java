package com.github.sco1237896.connector.core;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.apache.camel.CamelContext;
import org.apache.camel.main.BaseMainSupport;
import org.apache.camel.main.MainListener;
import org.apache.camel.main.MainListenerSupport;
import org.apache.camel.spi.CamelContextCustomizer;

@Recorder
public class ConnectorRecorder {
    public RuntimeValue<CamelContextCustomizer> createContextCustomizer(ConnectorConfig config) {
        return new RuntimeValue<>(new CamelContextCustomizer() {

            @Override
            public void configure(CamelContext camelContext) {
                camelContext.setStreamCaching(false);
            }
        });
    }

    public RuntimeValue<MainListener> createMainCustomizer(ConnectorConfig config) {
        return new RuntimeValue<>(new MainListenerSupport() {
            @Override
            public void beforeInitialize(BaseMainSupport main) {
            }
        });
    }
}
