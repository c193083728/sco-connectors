package com.github.sco1237896.connector.it.storage.minio

import io.minio.GetObjectArgs
import io.minio.PutObjectArgs
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

class MinioConnectorIT extends KafkaConnectorSpec {

    static MinioContainer minio

    @Override
    def setupSpec() {
        minio = new MinioContainer(network)
        minio.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(minio)
    }

    def "minio sink"() {
        setup:
            def payload = '''{ "username":"oscerd", "city":"Rome" }'''
            def objectName = UUID.randomUUID().toString()
            def topic = topic()
            def getArgs = GetObjectArgs.builder().bucket(topic).object(objectName).build()

            def cnt = forDefinition('minio_sink_v1.yaml')
                .withSourceProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                    'accessKey': minio.accessKey,
                    'secretKey':  minio.secretKey,
                    'endpoint': "http://${MinioContainer.CONTAINER_ALIAS}:${MinioContainer.PORT}",
                    'bucketName': topic,
                    'autoCreateBucket': 'true',
                ])
                .build()

            cnt.start()

            def mc = minio.client()
        when:
            kafka.send(topic, payload, ['file': objectName])
        then:
            await(10, TimeUnit.SECONDS) {
                try {
                    def stream = mc.getObject(getArgs)
                    def result = new String(stream.readAllBytes(), StandardCharsets.UTF_8)

                    return payload == result
                } catch (Exception e) {
                    return false
                }
            }
        cleanup:
            closeQuietly(cnt)
    }

    def "minio source"() {
        setup:
            def payload = '''{ "username":"oscerd", "city":"Rome" }'''
            def objectName = UUID.randomUUID().toString()
            def topic = topic()
            def getArgs = GetObjectArgs.builder().bucket(topic).object(objectName).build()


            def cnt = forDefinition('minio_source_v1.yaml')
                .withSinkProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSourceProperties([
                        'accessKey': minio.accessKey,
                        'secretKey':  minio.secretKey,
                        'endpoint': "http://${MinioContainer.CONTAINER_ALIAS}:${MinioContainer.PORT}",
                        'bucketName': topic,
                        'autoCreateBucket': 'true',
                        'deleteAfterRead': 'true',
                ])
                .build()

            cnt.start()
        when:
            try (InputStream is = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8))) {
                def object = PutObjectArgs.builder()
                        .bucket(topic)
                        .object(objectName)
                        .stream(is, is.available(), -1)
                        .build()

                minio.client().putObject(object)
            }
        then:
            await(10, TimeUnit.SECONDS) {
                def record = kafka.poll(cnt.containerId, topic).find {
                    it.value() == payload
                }

                return record != null
            }
        cleanup:
            closeQuietly(cnt)
    }

}
