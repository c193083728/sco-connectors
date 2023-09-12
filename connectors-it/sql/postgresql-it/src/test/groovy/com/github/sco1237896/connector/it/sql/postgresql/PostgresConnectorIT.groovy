package com.github.sco1237896.connector.it.sql.postgresql


import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.testcontainers.containers.PostgreSQLContainer

import java.util.concurrent.TimeUnit

@Slf4j
class PostgresConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = 'tc-postgres'

    static PostgreSQLContainer db

    @Override
    def setupSpec() {
        db = ContainerImages.container("container.image.postgres", PostgreSQLContainer.class)
        db.withLogConsumer(logger(CONTAINER_NAME))
        db.withNetwork(network)
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

            def cnt = forDefinition('postgresql_sink_v1.yaml')
                .withSourceProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                    'serverName': CONTAINER_NAME,
                    'serverPort': Integer.toString(PostgreSQLContainer.POSTGRESQL_PORT),
                    'username': db.username,
                    'password': db.password,
                    'query': 'INSERT INTO accounts (username,city) VALUES (:#username,:#city)',
                    'databaseName': db.databaseName
                ])
                .build()

            cnt.start()
        when:
            kafka.send(topic, payload)
        then:
            def records = kafka.poll(group, topic)
            records.size() == 1
            records.first().value() == payload

            await(30, TimeUnit.SECONDS) {
                return sql.rows("""SELECT * FROM accounts WHERE username='foo';""").size() == 1
            }

        cleanup:
            closeQuietly(sql)
            closeQuietly(cnt)
    }
}
