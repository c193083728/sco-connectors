package com.github.sco1237896.connector.it.sql.mariadb

import com.github.sco1237896.connector.it.support.KafkaContainer
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import org.testcontainers.containers.MariaDBContainer

import java.util.concurrent.TimeUnit

@Slf4j
class MariaDbConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = 'tc-mariadb'

    static MariaDBContainer db

    @Override
    def setupSpec() {
        db = ContainerImages.container("container.image.mariadb", MariaDBContainer.class)
        db.withLogConsumer(logger(CONTAINER_NAME))
        db.withNetwork(network)
        db.withNetworkAliases(CONTAINER_NAME)
        db.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(db)
    }

    def "mariadb sink"() {
        setup:
            def sql = Sql.newInstance(db.jdbcUrl,  db.username, db.password, db.driverClassName)
            def payload = '''{ "username":"oscerd", "city":"Rome" }'''

            sql.execute("""
                CREATE TABLE accounts (
                   username VARCHAR(50) UNIQUE NOT NULL,
                   city VARCHAR(50)
                );
            """)

            def topic = topic()
            def group = uid()

            def cnt = forDefinition('mariadb_sink_v1.yaml')
                .withSourceProperties([
                        'topic': topic,
                        'bootstrapServers': kafka.outsideBootstrapServers,
                        'consumerGroup': uid(),
                        'user': kafka.username,
                        'password': kafka.password,
                        'securityProtocol': KafkaContainer.SECURITY_PROTOCOL,
                        'saslMechanism': KafkaContainer.SASL_MECHANISM,
                ])
                .withSinkProperties([
                        'serverName': CONTAINER_NAME,
                        'serverPort': '3306',
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
                return sql.rows("""SELECT * FROM accounts WHERE username='oscerd';""").size() == 1
            }

        cleanup:
            closeQuietly(sql)
            closeQuietly(cnt)
    }
}
