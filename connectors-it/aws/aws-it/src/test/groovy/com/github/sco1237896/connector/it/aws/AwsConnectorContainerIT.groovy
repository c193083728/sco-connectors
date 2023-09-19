package com.github.sco1237896.connector.it.aws

import com.github.sco1237896.connector.it.support.SimpleConnectorSpec
import groovy.util.logging.Slf4j
import io.restassured.http.ContentType
import spock.lang.Ignore

@Slf4j
class AwsConnectorContainerIT extends SimpleConnectorSpec {

    def "container image exposes health and metrics"(String definition) {
        setup:
            def cnt = forDefinition(definition).build()
            cnt.start()
        when:
            def health = cnt.request.get('/q/health')
            def metrics = cnt.request.accept(ContentType.TEXT).get("/q/metrics")
        then:
            health.statusCode == 200
            metrics.statusCode == 200

            with (health.as(Map.class)) {
                status == 'UP'
                checks.find {
                    it.name == 'context' && it.status == 'UP'
                }
            }
        cleanup:
            closeQuietly(cnt)
        where:
            definition << [
                'aws_cloudwatch_sink_v1.yaml',
                'aws_ddb_sink_v1.yaml',
                'aws_ddb_stream_source_v1.yaml',
                'aws_kinesis_sink_v1.yaml',
                'aws_kinesis_source_v1.yaml',
                'aws_s3_sink_v1.yaml',
                'aws_s3_source_v1.yaml',
                'aws_ses_sink_v1.yaml',
                'aws_sns_sink_v1.yaml',
                'aws_sqs_sink_v1.yaml',
                'aws_sqs_source_v1.yaml',
                'aws_lambda_sink_v1.yaml',
            ]
    }
}
