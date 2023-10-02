package com.github.sco1237896.connector.it.messaging.jms

import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import com.github.sco1237896.connector.it.support.KafkaContainer
import groovy.util.logging.Slf4j
import jakarta.jms.BytesMessage
import jakarta.jms.Connection
import jakarta.jms.MessageConsumer
import jakarta.jms.Queue
import jakarta.jms.Session

import java.util.concurrent.TimeUnit

@Slf4j
class JmsConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = "tc-activemq"

    static ArtemisContainer mq

    @Override
    def setupSpec() {
        mq = ContainerImages.container("container.image.activemq-artemis", ArtemisContainer.class)
        mq.withLogConsumer(logger(CONTAINER_NAME))
        mq.withNetwork(network)
        mq.withNetworkAliases(CONTAINER_NAME)
        mq.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(mq)
    }

    def "jms-amqp sink"() {
        setup:
            Connection connection = mq.createAMQPConnection(mq)
            connection.start()

            def topic = topic()
            def group = uid()
            def payload = '''{ "value": "4", "suit": "hearts" }'''

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
            Queue queue = session.createQueue(topic)
            MessageConsumer consumer = session.createConsumer(queue)

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
                    "brokerURL": "amqp://${CONTAINER_NAME}:${ArtemisContainer.CONTAINER_PORT}",
                    "destinationName": topic
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

    def "jms-activemq sink"() {
        setup:
            Connection connection = mq.createActiveMqConnection(mq)
            connection.start()

            def topic = topic()
            def group = uid()
            def payload = '''{ "value": "4", "suit": "hearts" }'''

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
            Queue queue = session.createQueue(topic)
            MessageConsumer consumer = session.createConsumer(queue)

            def cnt = forDefinition('jms_apache_artemis_sink_v1.yaml')
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
                        "brokerURL": "tcp://${CONTAINER_NAME}:${ArtemisContainer.CONTAINER_PORT}",
                        "destinationName": topic
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
}
