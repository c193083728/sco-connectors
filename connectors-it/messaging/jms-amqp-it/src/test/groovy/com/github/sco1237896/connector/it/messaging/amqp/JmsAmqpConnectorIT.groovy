package com.github.sco1237896.connector.it.messaging.amqp

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.sco1237896.connector.it.support.AwaitStrategy
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import com.github.sco1237896.connector.it.support.KafkaContainer
import groovy.util.logging.Slf4j
import jakarta.jms.BytesMessage
import jakarta.jms.Connection
import jakarta.jms.JMSException
import jakarta.jms.MessageConsumer
import jakarta.jms.Queue
import jakarta.jms.Session
import org.apache.qpid.jms.JmsConnectionFactory
import org.testcontainers.containers.ContainerState
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.WaitStrategy
import spock.lang.Ignore

import java.util.concurrent.TimeUnit

@Ignore("TODO: this is failing when building from main, re-enable once fixed")
@Slf4j
class JmsAmqpConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = "tc-activemq"
    final static int CONTAINER_PORT = 61616

    static ObjectMapper mapper;
    static GenericContainer mq

    @Override
    def setupSpec() {
        mq = ContainerImages.container("container.image.activemq-artemis")
        mq.withLogConsumer(logger(CONTAINER_NAME))
        mq.withNetwork(network)
        mq.withNetworkAliases(CONTAINER_NAME)
        mq.withExposedPorts(CONTAINER_PORT)
        mq.withEnv('AMQ_USER', 'artemis')
        mq.withEnv('AMQ_PASSWORD', 'simetraehcapa')
        mq.withEnv('AMQ_EXTRA_ARGS', '--no-autotune')
        mq.waitingFor(waitForConnection())
        mq.start()

        mapper = new ObjectMapper()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(mq)
    }

    def "jms-amqp sink"() {
        setup:
            Connection connection = createConnection(mq)
            connection.start()

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
            Queue queue = session.createQueue("cards")
            MessageConsumer consumer = session.createConsumer(queue)

            def topic = topic()
            def group = uid()
            def payload = '''{ "value": "4", "suit": "hearts" }'''

            def cnt = forDefinition('jms_amqp_10_sink_v1.yaml')
                .withSourceProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': uid(),
                    'user': kafka.username,
                    'password': kafka.password,
                    'securityProtocol': KafkaContainer.SECURITY_PROTOCOL,
                    'saslMechanism': KafkaContainer.SASL_MECHANISM,
                ])
                .withSinkProperties([
                    "brokerURL": "amqp://${CONTAINER_NAME}:${CONTAINER_PORT}",
                    "destinationName": 'cards'
                ])
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload)
        then:
            def records = kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            await(20, TimeUnit.SECONDS) {
                BytesMessage message = consumer.receive() as BytesMessage
                byte[] bytes = message.getBody(byte[])

                if (bytes == null) {
                    return false
                }

                def actual = mapper.readTree(bytes)
                def expected = mapper.readTree(payload)

                return actual == expected
            }

        cleanup:
            closeQuietly(consumer)
            closeQuietly(session)
            closeQuietly(connection)
            closeQuietly(cnt)
    }


    static Connection createConnection(ContainerState state) {
        def remoteURI = "amqp://${state.host}:${state.getMappedPort(CONTAINER_PORT)}"
        def factory = new JmsConnectionFactory(remoteURI)

        return factory.createConnection()
    }

    static WaitStrategy waitForConnection() {
        return new AwaitStrategy() {
            @Override
            boolean ready() {
                try (Connection connection = createConnection(target)) {
                    connection.start()
                } catch (JMSException e) {
                    return false
                }

                return true
            }
        }
    }
}
