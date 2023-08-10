package com.github.sco1237896.connector.it.sql.sqlserver

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ContainerImages
import com.github.sco1237896.connector.it.support.KafkaConnectorSpec
import org.testcontainers.containers.MSSQLServerContainer

import java.util.concurrent.TimeUnit

@Slf4j
class ConnectorIT extends KafkaConnectorSpec {
    static MSSQLServerContainer db

    @Override
    def setupSpec() {
        db = ContainerImages.container("container.image.mssql", MSSQLServerContainer.class)
        db.acceptLicense()
        db.withLogConsumer(logger('tc-sqlserver'))
        db.withNetwork(KafkaConnectorSpec.network)
        db.withNetworkAliases('tc-sqlserver')
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

            def cnt = connectorContainer('sqlserver_sink_v1.json', [
                'kafka_topic' : topic,
                'kafka_bootstrap_servers': KafkaConnectorSpec.kafka.outsideBootstrapServers,
                'kafka_consumer_group': UUID.randomUUID().toString(),
                'db_server_name': 'tc-sqlserver',
                'db_server_port': Integer.toString(MSSQLServerContainer.MS_SQL_SERVER_PORT),
                'db_username': db.username,
                'db_password': db.password,
                'db_query': 'INSERT INTO connector.dbo.accounts (username,city) VALUES (:#username,:#city)',
                'db_database_name': 'connector',
                'db_encrypt': 'false'
            ])

            cnt.start()
        when:
        KafkaConnectorSpec.kafka.send(topic, payload)
        then:
            def records = KafkaConnectorSpec.kafka.poll(group, topic)
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
