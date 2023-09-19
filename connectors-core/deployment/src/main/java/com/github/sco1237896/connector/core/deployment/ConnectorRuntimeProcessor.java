package com.github.sco1237896.connector.core.deployment;

import com.github.sco1237896.connector.core.ConnectorConfig;
import com.github.sco1237896.connector.core.ConnectorRecorder;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import org.apache.camel.quarkus.core.deployment.main.spi.CamelMainListenerBuildItem;
import org.apache.camel.quarkus.core.deployment.spi.CamelContextCustomizerBuildItem;

public class ConnectorRuntimeProcessor {

    @Record(ExecutionTime.STATIC_INIT)
    @BuildStep
    CamelContextCustomizerBuildItem customizeContext(ConnectorRecorder recorder, ConnectorConfig config) {
        return new CamelContextCustomizerBuildItem(recorder.createContextCustomizer(config));
    }

    @Record(ExecutionTime.STATIC_INIT)
    @BuildStep
    CamelMainListenerBuildItem customizeMain(ConnectorRecorder recorder, ConnectorConfig config) {
        return new CamelMainListenerBuildItem(recorder.createMainCustomizer(config));
    }
}
