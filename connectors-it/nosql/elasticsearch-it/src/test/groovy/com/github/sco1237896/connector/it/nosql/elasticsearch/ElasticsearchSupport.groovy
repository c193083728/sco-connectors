package com.github.sco1237896.connector.it.nosql.elasticsearch

import com.github.sco1237896.connector.it.support.ConnectorSpecSupport
import com.github.sco1237896.connector.it.support.ContainerImages
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.testcontainers.containers.Network
import org.testcontainers.elasticsearch.ElasticsearchContainer

class ElasticsearchSupport {
    static final int ELASTIC_PORT = 9200

    static RestClient client(ElasticsearchContainer container, String user, String password) {
        def host = new HttpHost(container.host, container.getMappedPort(ELASTIC_PORT), "https")
        def builder = RestClient.builder(host)

        if (user != null && password != null) {
            def cp = new BasicCredentialsProvider()
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))

            builder.setHttpClientConfigCallback(hcb -> {
                return hcb.setDefaultCredentialsProvider(cp).setSSLContext(container.createSslContextFromCa())
            })
        }

        return builder.build()
    }

    static ElasticsearchContainer elasticsearchContainer(Network network, String alias, String user, String password) {
        def elastic = ContainerImages.container("container.image.elasticsearch", ElasticsearchContainer.class)
        elastic.withLogConsumer(ConnectorSpecSupport.logger(alias))
        elastic.withNetwork(network)
        elastic.withNetworkAliases(alias)
        elastic.withExposedPorts(ElasticsearchSupport.ELASTIC_PORT)

        if (user != null && password != null) {
            elastic.withPassword(password)
        }

        return elastic
    }
}
