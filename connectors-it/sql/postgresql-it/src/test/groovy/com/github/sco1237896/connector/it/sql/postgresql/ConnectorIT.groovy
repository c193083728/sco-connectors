package com.github.sco1237896.connector.it.sql.postgresql

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import com.github.sco1237896.connector.it.support.TestUtils
import org.testcontainers.containers.PostgreSQLContainer

import java.util.concurrent.TimeUnit

@Slf4j
class ConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = 'tc-postgres'

    static PostgreSQLContainer db

    @Override
    def setupSpec() {
        db = ContainerImages.container("container.image.postgres", PostgreSQLContainer.class)
        db.withLogConsumer(logger(CONTAINER_NAME))
        db.withNetwork(KafkaConnectorSpec.network)
        db.withNetworkAliases(CONTAINER_NAME)
        db.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(db)
    }

    def "postgresql sink"() {
        setup:
            def sql = Sql.newInstance(db.jdbcUrl,  db.username, db.password, db.driverClassName)
            def payload = '''{ "username":"foo", "city":"Rome" }'''

            sql.execute("""
                CREATE TABLE accounts (
                   username VARCHAR(50) UNIQUE NOT NULL,
                   city VARCHAR(50)
                );
            """)

            def topic = topic()
            def group = UUID.randomUUID().toString()

            def cnt = connectorContainer('postgresql_sink_v1.json', """
                kafka_topic : ${topic}
                kafka_bootstrap_servers: ${KafkaConnectorSpec.kafka.outsideBootstrapServers}
                kafka_consumer_group: ${UUID.randomUUID()}
                db_server_name: ${CONTAINER_NAME}
                db_server_port: ${PostgreSQLContainer.POSTGRESQL_PORT}
                db_username: ${db.username}
                db_password: ${db.password}
                db_query: 'INSERT INTO accounts (username,city) VALUES (:#username,:#city)'
                db_database_name: ${db.databaseName}
            """)

            cnt.start()
        when:
        KafkaConnectorSpec.kafka.send(topic, payload)
        then:
            def records = KafkaConnectorSpec.kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            await(30, TimeUnit.SECONDS) {
                return sql.rows("""SELECT * FROM accounts WHERE username='foo';""").size() == 1
            }

        cleanup:
            closeQuietly(sql)
            closeQuietly(cnt)
    }

    @spock.lang.Ignore("temporary disabled for CPaaS")
    def "postgresql sink with transform"() {
        setup:
            def sql = Sql.newInstance(db.jdbcUrl,  db.username, db.password, db.driverClassName)
            def payload = '''{ "username":"foo", "city":"Rome" }'''

            sql.execute("""
                CREATE TABLE accounts (
                   username VARCHAR(50) UNIQUE NOT NULL,
                   city VARCHAR(50)
                );
            """)

            def topic = topic()
            def group = UUID.randomUUID().toString()

            def cnt = connectorContainer('postgresql_sink_v1.json', """
                kafka_topic : ${topic}
                kafka_bootstrap_servers: ${KafkaConnectorSpec.kafka.outsideBootstrapServers}
                kafka_consumer_group: ${UUID.randomUUID()}
                db_server_name: ${CONTAINER_NAME}
                db_server_port: ${PostgreSQLContainer.POSTGRESQL_PORT}
                db_username: ${db.username}
                db_password: ${db.password}
                db_query: 'INSERT INTO accounts (username,city) VALUES (:#username,:#city)'
                db_database_name: ${db.databaseName}
                processors:
                - transform:
                    jq: '.username = "oscerd"'
            """)

            cnt.start()
        when:
        KafkaConnectorSpec.kafka.send(topic, payload)
        then:
            def records = KafkaConnectorSpec.kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            await(30, TimeUnit.SECONDS) {
                return sql.rows("""SELECT * FROM accounts WHERE username='oscerd';""").size() == 1
            }

        cleanup:
            closeQuietly(sql)
            closeQuietly(cnt)
    }

    def "postgresql sink with DLQ"() {
        setup:
            def sql = Sql.newInstance(db.jdbcUrl, db.username, db.password, db.driverClassName)
            def payload = '''{ "username":"oscerd", "city":"Rome" }'''

            def topic = topic()
            def errorHandlerTopic = super.topic();
            def group = UUID.randomUUID().toString()

            def cnt = connectorContainer('postgresql_sink_v1.json', [
                    'kafka_topic'            : topic,
                    'kafka_bootstrap_servers': KafkaConnectorSpec.kafka.outsideBootstrapServers,
                    'kafka_consumer_group'   : UUID.randomUUID().toString(),
                    'db_server_name'         : 'tc-postgres',
                    'db_server_port'         : Integer.toString(PostgreSQLContainer.POSTGRESQL_PORT),
                    'db_username'            : db.username,
                    'db_password'            : db.password,
                    'db_query'               : 'INSERT INTO accounts2 (username,city) VALUES (:#username,:#city)',
                    'db_database_name'       : db.databaseName
            ],
                    errorHandlerTopic,
                    false)

            cnt.start()
        when:
        KafkaConnectorSpec.kafka.send(topic, payload)
        then:
            def records = KafkaConnectorSpec.kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            def errorRecords = KafkaConnectorSpec.kafka.poll(group, errorHandlerTopic)
            errorRecords.size() == 1
            def actual = TestUtils.SLURPER.parseText(errorRecords.first().value())
            def expected = TestUtils.SLURPER.parseText(payload)
            actual == expected
        cleanup:
            closeQuietly(sql)
            closeQuietly(cnt)
    }

}
