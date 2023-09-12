package com.github.sco1237896.tools.maven.connector.support;

public final class ConnectorConstants {
    public static final String OPERATOR_RUNTIME = "camel-k";
    public static final String APPLICATION_PROPERTIES = "application.properties";

    public static final String CONNECTOR_TYPE_SOURCE = "source";
    public static final String CONNECTOR_TYPE_SINK = "sink";

    public static final String CAMEL_K_PROFILE_OPENSHIFT = "OpenShift";
    public static final String KAMEL_OPERATOR_ID = "camel.apache.org/operator.id";

    public static final String TRAIT_CAMEL_APACHE_ORG_ENV = "trait.camel.apache.org/environment.%s";
    public static final String TRAIT_CAMEL_APACHE_ORG_CONTAINER_IMAGE = "trait.camel.apache.org/container.image";
    public static final String TRAIT_CAMEL_APACHE_ORG_PROMETHEUS_ENABLED = "trait.camel.apache.org/prometheus.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_PROMETHEUS_POD_MONITOR = "trait.camel.apache.org/prometheus.pod-monitor";
    public static final String TRAIT_CAMEL_APACHE_ORG_KAMELETS_ENABLED = "trait.camel.apache.org/kamelets.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_JVM_ENABLED = "trait.camel.apache.org/jvm.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_LOGGING_JSON = "trait.camel.apache.org/logging.json";
    public static final String TRAIT_CAMEL_APACHE_ORG_OWNER_TARGET_LABELS = "trait.camel.apache.org/owner.target-labels";
    public static final String TRAIT_CAMEL_APACHE_ORG_OWNER_TARGET_ANNOTATIONS = "trait.camel.apache.org/owner.target-annotations";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_ENABLED = "trait.camel.apache.org/health.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_PROBE_ENABLED = "trait.camel.apache.org/health.liveness-probe-enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_PERIOD = "trait.camel.apache.org/health.liveness-period";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_TIMEOUT = "trait.camel.apache.org/health.liveness-timeout";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_SUCCESS_THRESHOLD = "trait.camel.apache.org/health.liveness-success-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_LIVENESS_FAILURE_THRESHOLD = "trait.camel.apache.org/health.liveness-failure-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_PROBE_ENABLED = "trait.camel.apache.org/health.readiness-probe-enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_PERIOD = "trait.camel.apache.org/health.readiness-period";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_TIMEOUT = "trait.camel.apache.org/health.readiness-timeout";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_SUCCESS_THRESHOLD = "trait.camel.apache.org/health.readiness-success-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_HEALTH_READINESS_FAILURE_THRESHOLD = "trait.camel.apache.org/health.readiness-failure-threshold";
    public static final String TRAIT_CAMEL_APACHE_ORG_DEPLOYMENT_ENABLED = "trait.camel.apache.org/deployment.enabled";
    public static final String TRAIT_CAMEL_APACHE_ORG_DEPLOYMENT_STRATEGY = "trait.camel.apache.org/deployment.strategy";

    public static final String CONNECTOR_REVISION = "sco1237896.github.com/connector.revision";

    private ConnectorConstants() {
    }
}
