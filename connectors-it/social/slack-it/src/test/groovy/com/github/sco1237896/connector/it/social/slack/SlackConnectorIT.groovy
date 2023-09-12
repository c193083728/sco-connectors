package com.github.sco1237896.connector.it.social.slack

import groovy.util.logging.Slf4j
import io.restassured.RestAssured
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import com.github.sco1237896.connector.it.support.TestUtils
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

@IgnoreIf({
    !hasEnv('SLACK_TEST_CHANNEL') ||
    !hasEnv('SLACK_TEST_TOKEN'  ) ||
    !hasEnv('SLACK_TEST_WEBHOOK')
})
@Slf4j
class SlackConnectorIT extends KafkaConnectorSpec {

    def "slack source"() {
        given:
            def topic = topic()
            def group = UUID.randomUUID().toString()
            def count = 10
            def target = URI.create(System.getenv('SLACK_TEST_WEBHOOK'))
            def messages = new TreeSet<String>()


            def cnt = forDefinition('slack_source_v1.yaml')
                .withSinkProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSourceProperties([
                        'channel': System.getenv('SLACK_TEST_CHANNEL'),
                        "token": System.getenv('SLACK_TEST_TOKEN'),
                        'delay': '5s'
                ])
                .build()

            cnt.start()
        when:
            for (int i = 0; i < count ; i++) {
                messages << "slack-event ${topic}/${i}".toString()

                RestAssured
                    .given()
                        .contentType('application/json')
                        .accept('application/json')
                        .body("""{ "text": "${messages[i]}" }""")
                    .when()
                        .post(target)
                    .then()
                        .assertThat().statusCode(200)

                // add some sleep to avoid throttling
                Thread.sleep(1000)
            }
        then:
            def kc = kafka.consumer(group, topic)

            await(60, TimeUnit.SECONDS) {
                kafka.poll(kc).forEach(r -> {
                    def result = TestUtils.SLURPER.parseText(r.value())
                    def text = result.text.toString()

                    if (messages.remove(text)) {
                        log.info('Processing {}, {}', messages.size(), text)
                    }
                })

                return messages.isEmpty()
            }

        cleanup:
            closeQuietly(kc)
            closeQuietly(cnt)
    }
}
