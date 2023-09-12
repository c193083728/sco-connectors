package com.github.sco1237896.connector.it.nosql.mongodb

import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import com.mongodb.client.MongoClients
import com.mongodb.client.model.CreateCollectionOptions
import groovy.util.logging.Slf4j
import org.bson.Document
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.SelinuxContext
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

import static com.mongodb.client.model.Filters.and
import static com.mongodb.client.model.Filters.eq

@Slf4j
class MongoConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = "tc-mongodb"
    final static int CONTAINER_PORT = 27017
    
    static MongoDBContainer db

    static final String JKS_PATH = '/etc/wm/mongo-test.pem'
    static final String JKS_CLASSPATH = '/ssl/mongo-test.pem'
    static final String TLS_MODE = 'allowTLS'

    @Override
    def setupSpec() {
        db = ContainerImages.container("container.image.mongodb", MongoDBContainer.class)
        db.withLogConsumer(logger(CONTAINER_NAME))
        db.withNetwork(network)
        db.withNetworkAliases(CONTAINER_NAME)
        db.withClasspathResourceMapping(JKS_CLASSPATH, JKS_PATH, BindMode.READ_ONLY, SelinuxContext.SHARED)

        db.withCommand(
            "--tlsMode", "${TLS_MODE}",
            "--tlsCertificateKeyFile", "${JKS_PATH}",
            "--tlsAllowInvalidHostnames",
            "--tlsAllowInvalidCertificates",
            "--tlsAllowConnectionsWithoutCertificates")

        db.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(db)
    }

    @Unroll
    def "mongodb sink (#ssl)"(boolean ssl) {
        setup:
            def mongoClient = MongoClients.create(db.replicaSetUrl + "?tlsAllowInvalidHostnames=true")
            def database = mongoClient.getDatabase("toys")
            def collection = database.getCollection("cards")

            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = "{ \"value\": ${ssl}, \"suit\": \"hearts\" }"

            def cnt = forDefinition('mongodb_sink_v1.yaml')
                .withSourceProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                        'hosts': "${CONTAINER_NAME}:${CONTAINER_PORT}",
                        'collection': collection.getNamespace().getCollectionName(),
                        'createCollection': 'true',
                        'database': database.getName(),
                        'ssl': ssl,
                        'sslValidationEnabled': !ssl
                ])
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload)
        then:
            def records = kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            await(10, TimeUnit.SECONDS) {
                return collection.countDocuments(and(eq('value', ssl), eq('suit', 'hearts'))) == 1
            }

        cleanup:
            closeQuietly(mongoClient)
            closeQuietly(cnt)
        where:
            ssl << [true, false]
    }

    @Unroll
    def "mongodb source (#ssl)"(boolean ssl) {
        setup:
            def mongoClient = MongoClients.create(db.replicaSetUrl + "?tlsAllowInvalidHostnames=true")
            def database = mongoClient.getDatabase("toys")
            if(database.listCollectionNames().every {return !"boardgames".equals(it);}) {
                database.createCollection("boardgames", new CreateCollectionOptions().capped(true).sizeInBytes(256))
            }
            def collection = database.getCollection("boardgames", Document.class)

            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = "{ \"owned\": ${ssl}, \"title\": \"powergrid\" }"

            def cnt = forDefinition('mongodb_source_v1.yaml')
                .withSinkProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSourceProperties([
                        'hosts': "${CONTAINER_NAME}:${CONTAINER_PORT}",
                        'collection': collection.getNamespace().getCollectionName(),
                        'database': database.getName(),
                        'ssl': ssl,
                        'sslValidationEnabled': !ssl
                ])
                .build()

            cnt.start()
        when:
           collection.insertOne(Document.parse(payload))
        then:
            await(10, TimeUnit.SECONDS) {
                def records = kafka.poll(group, topic)
                records.any {
                    def jsonDoc = Document.parse(it.value())
                    return jsonDoc.getBoolean("owned") == ssl && "powergrid".equals(jsonDoc.getString("title"))
                }
            }
        cleanup:
            closeQuietly(mongoClient)
            closeQuietly(cnt)
        where:
            ssl << [true, false]
    }

}
