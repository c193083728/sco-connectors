package com.github.sco1237896.connector.core.deployment;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.List;
import java.util.Map;

@ConfigMapping(prefix = "quarkus.sco")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface ConnectorConfiguration {

    /**
     * The catalog.
     */
    Catalog catalog();

    /**
     * The container.
     */
    Container container();

    /**
     * The kamelets.
     */
    Kamelets kamelets();

    /**
     * The connector.
     */
    Connector connector();

    interface Connector {
        /**
         * The type.
         */
        String type();

        /**
         * The version.
         */
        String version();

        /**
         * The definitions.
         */
        List<ConnectorDefinition> definitions();

        /**
         * The deployment.
         */
        Deployment deployment();
    }

    interface Catalog {
        /**
         * The root.
         */
        String root();

        /**
         * The group.
         */
        String group();

        /**
         * The initial revision.
         */
        @WithName("initial-revision")
        int initialRevision();
    }

    interface Container {

        /**
         * The registry.
         */
        String registry();

        /**
         * The organization.
         */
        String organization();

        /**
         * The tag.
         */
        String tag();

        /**
         * The image prefix.
         */
        @WithName("image-prefix")
        String imagePrefix();

        /**
         * The image base.
         */
        @WithName("image-base")
        String imageBae();
    }

    interface Deployment {
        /**
         * The deadline
         */
        String deadline();

        /**
         * The request
         */
        Resource request();
    }

    interface Resource {
        /**
         * The cpu
         */
        String cpu();

        /**
         * The memory
         */
        String memory();
    }

    interface Kamelets {
        /**
         * The version.
         */
        String version();
    }

    interface ConnectorDefinition {
        /**
         * The name
         */
        String name();

        /**
         * The description
         */
        String description();

        /**
         * The title
         */
        String title();

        /**
         * The group
         */
        @WithDefault("${quarkus.sco.catalog.group}")
        String group();

        /**
         * The version
         */
        @WithDefault("${quarkus.sco.connector.version}")
        String version();

        /**
         * The source
         */
        KameletRef source();

        /**
         * The sink
         */
        KameletRef sink();

        /**
         * The annotation
         */
        Map<String, String> annotations();
    }

    interface KameletRef {
        /**
         * The name
         */
        String name();

        /**
         * The version
         */

        @WithDefault("${quarkus.sco.kamelets.version}")
        String version();
    }
}
