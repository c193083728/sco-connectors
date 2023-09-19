package com.github.sco1237896.connector.core.deployment.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.sco1237896.connector.core.deployment.support.ConnectorSupport;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

import java.util.TreeMap;

@Group(ConnectorSupport.API_GROUP)
@Version(value = ConnectorSupport.API_VERSION)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectorDefinition extends CustomResource<ConnectorDefinitionSpec, ConnectorDefinitionStatus> {

    public ConnectorDefinition() {
        super();

        getMetadata().setAnnotations(new TreeMap<>());
    }

    @Override
    protected ConnectorDefinitionSpec initSpec() {
        return new ConnectorDefinitionSpec();
    }
}
