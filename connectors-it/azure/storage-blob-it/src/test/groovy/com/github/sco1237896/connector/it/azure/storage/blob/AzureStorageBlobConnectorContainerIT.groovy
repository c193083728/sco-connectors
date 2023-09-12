package com.github.sco1237896.connector.it.azure.storage.blob

import groovy.util.logging.Slf4j
import com.github.sco1237896.connector.it.support.ConnectorContainer
import com.github.sco1237896.connector.it.support.SimpleConnectorSpec

@Slf4j
class AzureStorageBlobConnectorContainerIT extends SimpleConnectorSpec {

    def "container image exposes health and metrics"(String definition) {
        setup:
            def cnt = forDefinition(definition).build()
            cnt.start()
        when:
            def health = cnt.request.get('/q/health')
            def metrics = cnt.request.get("/q/metrics")
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
                'azure_storage_blob_source_v1.yaml',
                'azure_storage_blob_sink_v1.yaml'
            ]
    }
}
