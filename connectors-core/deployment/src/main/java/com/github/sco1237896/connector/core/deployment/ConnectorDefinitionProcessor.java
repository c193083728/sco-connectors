package com.github.sco1237896.connector.core.deployment;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.sco1237896.connector.core.deployment.model.ConnectorDefinition;
import com.github.sco1237896.connector.core.deployment.spi.ConnectorDefinitionBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.KameletBuildItem;
import com.github.sco1237896.connector.core.deployment.spi.PomEnricherBuildItem;
import com.github.sco1237896.connector.core.deployment.support.ConnectorSupport;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import org.apache.camel.v1.Kamelet;
import org.apache.camel.v1.Pipe;
import org.apache.camel.v1.PipeSpec;
import org.apache.camel.v1.pipespec.ErrorHandler;
import org.apache.camel.v1.pipespec.Sink;
import org.apache.camel.v1.pipespec.Source;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConnectorDefinitionProcessor {
    /**
     *
     * @param configuration the connector configuration
     * @param kamelets      the list of kamelet known by the project
     * @param definitions   the connectors definitions
     */
    @Consume(PomEnricherBuildItem.class)
    @BuildStep
    void generateDefinitions(
            ConnectorConfiguration configuration,
            List<KameletBuildItem> kamelets,
            BuildProducer<ConnectorDefinitionBuildItem> definitions) {

        for (ConnectorConfiguration.ConnectorDefinition connector : configuration.connector().definitions()) {
            definitions.produce(
                    generateDefinition(configuration, connector, kamelets));
        }
    }

    private ConnectorDefinitionBuildItem generateDefinition(
            ConnectorConfiguration configuration,
            ConnectorConfiguration.ConnectorDefinition connector,
            List<KameletBuildItem> kamelets) {

        final String id = connector.name().replace("-", "_");

        ObjectNode sourceKamelet = ConnectorSupport.lookupKamelet(connector.source(), kamelets)
                .map(ConnectorSupport::cleanupKamelet)
                .orElseThrow(() -> new RuntimeException(
                        "Unable to find source kamelet " + connector.source().name() + ", version: "
                                + connector.source().version()));
        ObjectNode sinkKamelet = ConnectorSupport.lookupKamelet(connector.sink(), kamelets)
                .map(ConnectorSupport::cleanupKamelet)
                .orElseThrow(() -> new RuntimeException(
                        "Unable to find sink kamelet " + connector.sink().name() + ", version: "
                                + connector.source().version()));

        final Pipe pipe = new Pipe();
        pipe.setMetadata(new ObjectMeta());
        pipe.getMetadata().setAnnotations(new TreeMap<>());
        pipe.setSpec(new PipeSpec());

        var srcRef = new org.apache.camel.v1.pipespec.source.Ref();
        srcRef.setApiVersion(HasMetadata.getApiVersion(Kamelet.class));
        srcRef.setKind(HasMetadata.getKind(Kamelet.class));
        srcRef.setName(connector.source().name());

        Source src = new Source();
        src.setRef(srcRef);

        pipe.getSpec().setSource(src);

        var snkRef = new org.apache.camel.v1.pipespec.sink.Ref();
        snkRef.setApiVersion(HasMetadata.getApiVersion(Kamelet.class));
        snkRef.setKind(HasMetadata.getKind(Kamelet.class));
        snkRef.setName(connector.sink().name());

        Sink snk = new Sink();
        snk.setRef(snkRef);

        pipe.getSpec().setSink(snk);

        // sco
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/connector.group", connector.group());
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/connector.id", id);
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/connector.title", connector.title());
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/connector.description", connector.description());
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/connector.version", connector.version());
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/catalog.id", configuration.connector().type().replace("-", "_"));
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.API_GROUP + "/catalog.group", configuration.catalog().group());

        // camel
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.TRAIT_CAMEL_APACHE_ORG_DEPLOYMENT_PROGRESS_DEADLINE,
                configuration.connector().deployment().deadline());
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.TRAIT_CAMEL_APACHE_ORG_CONTAINER_REQUEST_CPU,
                configuration.connector().deployment().request().cpu());
        pipe.getMetadata().getAnnotations().put(
                ConnectorSupport.TRAIT_CAMEL_APACHE_ORG_CONTAINER_REQUEST_MEMORY,
                configuration.connector().deployment().request().memory());

        ErrorHandler eh = new ErrorHandler();
        eh.setAdditionalProperty("log", Map.of());

        pipe.getSpec().setErrorHandler(eh);

        connector.annotations().forEach((k, v) -> {
            pipe.getMetadata().getAnnotations().putIfAbsent(ConnectorSupport.API_GROUP + "/" + k, v);
        });

        ConnectorDefinition definition = new ConnectorDefinition();
        definition.getMetadata().setName(id);
        definition.getSpec().setDefinition(pipe);
        definition.getSpec().getResources().add(sourceKamelet);
        definition.getSpec().getResources().add(sinkKamelet);

        return new ConnectorDefinitionBuildItem(definition);
    }
}
