package com.github.sco1237896.connector.it.nosql.elasticsearch

import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import com.github.sco1237896.connector.it.support.KafkaContainer
import com.github.sco1237896.connector.it.support.TestUtils
import groovy.util.logging.Slf4j
import org.elasticsearch.client.Request
import spock.lang.Ignore

import java.util.concurrent.TimeUnit

import static ElasticsearchSupport.client
import static ElasticsearchSupport.elasticsearchContainer

@Slf4j
class ElasticsearchConnectorIT extends KafkaConnectorSpec {
    private static final String ELASTIC_SECURED_USER = 'elastic'
    private static final String ELASTIC_SECURED_PASSWORD = 'supersecret'
    private static final String ELASTIC_ALIAS = 'tc-elastic'

    // TODO we are likely needed to add custom certificate
    @Ignore("Fails because the host name 'tc-elastic' does not match the certificate subject provided by the peer")
    def "elasticsearch sink"() {
        setup:
            def elastic = elasticsearchContainer(network, ELASTIC_ALIAS, ELASTIC_SECURED_USER, ELASTIC_SECURED_PASSWORD)
            elastic.start()

            def topic = topic()
            def payload = """{ "kafka_topic": "${topic}" }"""
            def client = client(elastic, ELASTIC_SECURED_USER, ELASTIC_SECURED_PASSWORD)

            def get = new Request("GET", "/${topic}")
            def delete = new Request("DELETE", "/${topic}")
            def search = new Request("GET", "/${topic}/_search")

            def caCet = elastic.caCertAsBytes().map(Base64.encoder::encodeToString).orElseThrow {
                new RuntimeException('Unable to get CA Cart Bytes')
            }

            def props = [
                'hostAddresses': ELASTIC_ALIAS,
                'clusterName': topic,
                'certificate': caCet,
                'user': ELASTIC_SECURED_USER,
                'password': ELASTIC_SECURED_PASSWORD
            ]

            def cnt = forDefinition('elasticsearch_index_sink_v1.yaml')
                .withSourceProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': uid(),
                        'user': kafka.username,
                        'password': kafka.password,
                        'securityProtocol': KafkaContainer.SECURITY_PROTOCOL,
                        'saslMechanism': KafkaContainer.SASL_MECHANISM,
                ])
                .withSinkProperties(props)
                .withUserProperty('quarkus.log.category."org.apache.camel.component.elasticsearch".level', 'DEBUG')
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload, [
                'indexId': topic,
                'indexName': topic
            ])

        then:
            def records = kafka.poll(topic)
            records.size() == 1
            records.first().value() == payload

            until(10, TimeUnit.SECONDS) {
                try {
                    def r = client.performRequest(get)
                    if (r.statusLine.statusCode != 200) {
                        return false
                    }

                    def s = client.performRequest(search)
                    if (r.statusLine.statusCode != 200) {
                        return false
                    }

                    try (InputStream is = s.entity.content) {
                        def doc = TestUtils.SLURPER.parse(is)
                        if (doc.hits.total.value != 1) {
                            return false
                        }

                        return doc.hits.hits[0]._source.kafka_topic == topic
                    }
                } catch(Exception e) {
                    log.info('Failed to invoke elasticsearch endpoint, will retry (reason: {})', e.message)
                    return false
                }
            }

        cleanup:
            if (client != null) {
                client.performRequest(delete)
            }

            closeQuietly(cnt)
            closeQuietly(elastic)
    }
}
