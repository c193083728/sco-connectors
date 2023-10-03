package com.github.sco1237896.connector.it.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

public class ArtemisContainer extends GenericContainer<ArtemisContainer> {
    public static final int CONTAINER_PORT = 61616;
    public static final String ARTEMIS_USERNAME = "artemis";
    public static final String ARTEMIS_PASSWORD = "simetraehcapa";

    public ArtemisContainer(DockerImageName imageName) {
        super(imageName);

        Set<String> extraArgs = Set.of(
                "--relax-jolokia",
                "--mapped",
                "--no-autotune",
                "--no-fsync",
                "--no-hornetq-acceptor",
                "--no-mqtt-acceptor",
                "--no-stomp-acceptor",
                "--no-web");

        withExposedPorts(CONTAINER_PORT);
        withEnv("ARTEMIS_USER", ARTEMIS_USERNAME);
        withEnv("ARTEMIS_PASSWORD", ARTEMIS_PASSWORD);
        withEnv("ANONYMOUS_LOGIN", "true");
        withEnv("EXTRA_ARGS", String.join(" ", extraArgs));
        waitingFor(Wait.forLogMessage(".*Server is now live.*", 1));
    }

    public static int getContainerPort() {
        return CONTAINER_PORT;
    }

    public static String getUsername() {
        return ARTEMIS_USERNAME;
    }

    public static String getPassword() {
        return ARTEMIS_PASSWORD;
    }

    public Connection createActiveMqConnection(ContainerState state) throws JMSException {
        String remoteURI = String.format("tcp://%s:%d", state.getHost(), state.getMappedPort(ArtemisContainer.CONTAINER_PORT));
        ConnectionFactory factory = new ActiveMQConnectionFactory(remoteURI);

        return factory.createConnection();
    }

    public Connection createAMQPConnection(ContainerState state) throws JMSException {
        String remoteURI = String.format("amqp://%s:%d", state.getHost(), state.getMappedPort(ArtemisContainer.CONTAINER_PORT));
        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);

        return factory.createConnection();
    }
}
