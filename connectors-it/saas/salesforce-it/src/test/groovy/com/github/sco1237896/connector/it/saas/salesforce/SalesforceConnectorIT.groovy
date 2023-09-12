package com.github.sco1237896.connector.it.saas.salesforce


import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import spock.lang.IgnoreIf
import spock.lang.Stepwise

import java.util.concurrent.TimeUnit

@IgnoreIf({
    !hasEnv('SF_CLIENT_ID'      ) ||
    !hasEnv('SF_CLIENT_SECRET'  ) ||
    !hasEnv('SF_CLIENT_USERNAME') ||
    !hasEnv('SF_CLIENT_PASSWORD')
})
// steps musts be executed in order (create, update, delete)
@Stepwise
class SalesforceConnectorIT extends KafkaConnectorSpec {
    static def sObjectId

    def "salesforce create sink"() {
        setup:
            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = """{ "Name" : "${group}" }"""

             def cnt = forDefinition('create_sink_v1.yaml')
                .withSourceProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                    'clientId': System.getenv('SF_CLIENT_ID'),
                    'clientSecret': System.getenv('SF_CLIENT_SECRET'),
                    'userName': System.getenv('SF_CLIENT_USERNAME'),
                    'password': System.getenv('SF_CLIENT_PASSWORD'),
                    'sObjectName': 'Account'
                ])
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload)
        then:
            await(30, 1, TimeUnit.SECONDS, () -> {
                def result = SalesforceConnectorSupport.query("SELECT name,id from Account WHERE name='${group}'")

                if (result?.totalSize != 1) {
                    return false
                }

                sObjectId = result?.records[0].Id

                return sObjectId != null
            })

        cleanup:
            closeQuietly(cnt)
    }

    def "salesforce update sink"() {
        setup:
            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = """{ "Name" : "${group}" }"""

            def cnt = forDefinition('update_sink_v1.yaml')
                .withSourceProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                        'clientId': System.getenv('SF_CLIENT_ID'),
                        'clientSecret': System.getenv('SF_CLIENT_SECRET'),
                        'userName': System.getenv('SF_CLIENT_USERNAME'),
                        'password': System.getenv('SF_CLIENT_PASSWORD'),
                        'sObjectName': 'Account',
                        'sObjectId': sObjectId
                ])
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload)
        then:
            await(30, 1, TimeUnit.SECONDS, () -> {
                def result = SalesforceConnectorSupport.query("SELECT name,id from Account WHERE id='${sObjectId}'")
                return result?.totalSize == 1 && result?.records[0].Name == group
            })

        cleanup:
            closeQuietly(cnt)
    }

    def "salesforce delete sink"() {
        setup:
            def topic = topic()
            def payload = """{ "sObjectId" : "${sObjectId}", "sObjectName": "Account" }"""


             def cnt = forDefinition('delete_sink_v1.yaml')
                .withSourceProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                        'clientId': System.getenv('SF_CLIENT_ID'),
                        'clientSecret': System.getenv('SF_CLIENT_SECRET'),
                        'userName': System.getenv('SF_CLIENT_USERNAME'),
                        'password': System.getenv('SF_CLIENT_PASSWORD'),
                ])
                .build()

            cnt.start()
        when:
         kafka.send(topic, payload)
        then:
            await(30, 1, TimeUnit.SECONDS, () -> {
                def result = SalesforceConnectorSupport.query("SELECT name,id from Account WHERE id='${sObjectId}'")
                return result?.totalSize == 0
            })

        cleanup:
            closeQuietly(cnt)
    }
}
