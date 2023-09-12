package com.github.sco1237896.connector.it.sql.sqlserver

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import org.testcontainers.containers.MSSQLServerContainer

import java.util.concurrent.TimeUnit

@Slf4j
class SqlServerConnectorIT extends KafkaConnectorSpec {
    final static String CONTAINER_NAME = 'tc-sqlserver'

    static MSSQLServerContainer db

    @Override
    def setupSpec() {
        db = ContainerImages.container("container.image.mssql", MSSQLServerContainer.class)
        db.acceptLicense()
        db.withLogConsumer(logger(CONTAINER_NAME))
        db.withNetwork(network)
        db.withNetworkAliases(CONTAINER_NAME)
        db.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(db)
    }

    def "sqlserver sink"() {
        setup:
            def sql = Sql.newInstance(db.jdbcUrl,  db.username, db.password, db.driverClassName)
            def payload = '''{ "username":"oscerd", "city":"Rome" }'''

            sql.execute('CREATE DATABASE connector')
            sql.execute('CREATE TABLE connector.dbo.accounts (username VARCHAR(50) UNIQUE NOT NULL, city VARCHAR(50))')

            def topic = topic()
            def group = UUID.randomUUID().toString()



            def cnt = forDefinition('sqlserver_sink_v1.yaml')
                .withSourceProperties([
                    'topic': topic,
                    'bootstrapServers': kafka.outsideBootstrapServers,
                    'consumerGroup': UUID.randomUUID().toString(),
                ])
                .withSinkProperties([
                    'serverName': CONTAINER_NAME,
                    'serverPort': Integer.toString(MSSQLServerContainer.MS_SQL_SERVER_PORT),
                    'username': db.username,
                    'password': db.password,
                    'query': 'INSERT INTO connector.dbo.accounts (username,city) VALUES (:#username,:#city)',
                    'databaseName': 'connector',
                    'encrypt': 'false'
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
                return sql.rows("""SELECT * FROM connector.dbo.accounts WHERE username='oscerd';""").size() == 1
            }

        cleanup:
            closeQuietly(sql)
            closeQuietly(cnt)
    }
}
