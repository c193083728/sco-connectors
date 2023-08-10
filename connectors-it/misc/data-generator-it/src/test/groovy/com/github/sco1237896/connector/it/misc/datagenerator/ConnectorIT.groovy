package com.github.sco1237896.connector.it.misc.datagenerator

import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import spock.lang.Unroll

@Slf4j
class ConnectorIT extends KafkaConnectorSpec {
    @Unroll
    def "data-generator source"() {
        setup:
            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = '''{ "foo": "bar" }'''

            def cnt = connectorContainer('data_generator_source_v1.json', [
                'kafka_topic' : topic,
                'kafka_bootstrap_servers': KafkaConnectorSpec.kafka.outsideBootstrapServers,
                'kafka_consumer_group': UUID.randomUUID().toString(),
                'timer_period': '1s',
                'timer_message': payload,
                'timer_content_type': 'application/json',
            ])

            cnt.withCamelComponentDebugEnv()
            cnt.start()
        when:
            cnt.start()
        then:
            def records = KafkaConnectorSpec.kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload
        cleanup:
            closeQuietly(cnt)
    }
}
