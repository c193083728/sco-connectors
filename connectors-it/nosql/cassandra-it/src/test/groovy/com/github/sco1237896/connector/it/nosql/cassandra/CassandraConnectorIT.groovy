package com.github.sco1237896.connector.it.nosql.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.DataType
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import org.testcontainers.containers.CassandraContainer

import java.util.concurrent.TimeUnit


@Slf4j
class CassandraConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = 'tc-cassandra'
    final static int CONTAINER_PORT = 9042
    
    static CassandraContainer cassandra
    public static final String TEST_KSP = "test_ksp"
    public static final String TEST_TABLE = "test_data"

    @Override
    def setupSpec() {
        cassandra = ContainerImages.container("container.image.cassandra", CassandraContainer.class)
        cassandra.withLogConsumer(logger(CONTAINER_NAME))
        cassandra.withNetwork(network)
        cassandra.withNetworkAliases(CONTAINER_NAME)
        cassandra.start()

        Cluster cluster = cassandra.getCluster();

        try(Session session = cluster.connect()) {
            Statement keyspaceStm = SchemaBuilder.createKeyspace(TEST_KSP)
                    .ifNotExists().with().replication(Map.of("class", "SimpleStrategy", "replication_factor", "1"))

            session.execute(keyspaceStm)
        }

        try(Session session = cluster.connect(TEST_KSP)) {
            Statement tableStm = SchemaBuilder.createTable(TEST_TABLE)
                    .addPartitionKey("id", DataType.timeuuid())
                    .addClusteringColumn("text", DataType.text())

            session.execute(tableStm)
        }
    }

    @Override
    def cleanupSpec() {
        closeQuietly(cassandra)
    }

    def "cassandra sink"() {
        setup:
            def topic = topic()
            def group = UUID.randomUUID().toString()
            def payload = '''["ciao"]'''


            def cnt = forDefinition('cassandra_sink_v1.yaml')
                .withSourceProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                    'connectionHost': CONTAINER_NAME,
                    'connectionPort': "${CONTAINER_PORT}",
                    'keyspace': TEST_KSP,
                    'query': 'insert into ' + TEST_TABLE + '(id, text) values (now(), ?)'
                ])
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload)
        then:
            await(10, TimeUnit.SECONDS) {
                Cluster cluster = cassandra.getCluster()

                try(Session session = cluster.connect(TEST_KSP)) {
                    ResultSet rs = session.execute("select count(*) from " + TEST_TABLE + ";")

                    return rs.all().size() == 1
                } catch (Exception e) {
                    return false
                }
            }

        cleanup:
            closeQuietly(cnt)
    }

    def "cassandra source"() {
        setup:
            def topic = topic()
            def payload = 'ciao'


            def cnt = forDefinition('cassandra_source_v1.yaml')
                .withSinkProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSourceProperties([
                    'connectionHost': CONTAINER_NAME,
                    'connectionPort': "${CONTAINER_PORT}",
                    'keyspace': TEST_KSP,
                    'query': 'select text from '+ TEST_TABLE
                ])
                .build()

            cnt.start()
        when:
            Cluster cluster = cassandra.getCluster()

            try(Session session = cluster.connect(TEST_KSP)) {
                session.execute("insert into " + TEST_TABLE + "(id, text) values (now(), '" + payload + "');")
            }
         then:
         await(10, TimeUnit.SECONDS) {
             def record = kafka.poll(cnt.containerId, topic).find {
                 it.value().contains(payload)
             }

             return record != null
         }

        cleanup:
            closeQuietly(cnt)
    }
}
