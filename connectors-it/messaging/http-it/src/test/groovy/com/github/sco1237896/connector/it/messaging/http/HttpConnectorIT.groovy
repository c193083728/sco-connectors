package com.github.sco1237896.connector.it.messaging.http

import com.github.tomakehurst.wiremock.client.WireMock
import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

import static com.github.tomakehurst.wiremock.client.WireMock.absent
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo
import static com.github.tomakehurst.wiremock.client.WireMock.ok
import static com.github.tomakehurst.wiremock.client.WireMock.post
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import static com.github.tomakehurst.wiremock.client.WireMock.verify

@Slf4j
class HttpConnectorIT extends KafkaConnectorSpec {
    static final int PORT = 8080
    static final String SCHEME = 'http'
    static final String HOST = 'tc-mock'

    static GenericContainer mock

    @Override
    def setupSpec() {
        mock = ContainerImages.container("container.image.wiremock")
        mock.withLogConsumer(logger(HOST))
        mock.withNetwork(network)
        mock.withNetworkAliases(HOST)
        mock.withExposedPorts(PORT)
        mock.waitingFor(Wait.forListeningPort())
        mock.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(mock)
    }

    @Unroll
    def "http sink"() {
        setup:
            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = '''{ "foo": "bar" }'''

            def path = urlPathEqualTo("/run")
            def response = ok().withBody(''' { "foo": "baz" }''')
            def request = post(path)
                .withHeader('x-test-header', equalTo(group))
                .withHeader('kafka.TOPIC', absent())
                .withHeader('kafka.PARTITION', absent())
                .withHeader('kafka.TIMESTAMP', absent())
                .withHeader('kafka.OFFSET', absent())

            WireMock.configureFor(SCHEME, mock.getHost(), mock.getMappedPort(PORT))
            WireMock.stubFor(request.willReturn(response));


            def cnt = forDefinition('http_sink_v1.yaml')
                .withSourceProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                    'method': 'POST',
                    'url': "${SCHEME}://${HOST}:${PORT}/run".toString(),
                ])
                .build()

            cnt.withCamelComponentDebugEnv()
            cnt.start()
        when:
            kafka.send(topic, payload, [
                'x-test-header': group
            ])
        then:
            def records = kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            untilAsserted(5, TimeUnit.SECONDS) {
                verify(1, postRequestedFor(path))
            }

            assert WireMock.findUnmatchedRequests().isEmpty()
        cleanup:
            closeQuietly(cnt)
    }
}
