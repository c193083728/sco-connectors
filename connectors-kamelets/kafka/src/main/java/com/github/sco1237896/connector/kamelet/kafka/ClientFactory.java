package com.github.sco1237896.connector.kamelet.kafka;

import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.util.ObjectHelper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@SuppressWarnings("unused")
public class ClientFactory extends DefaultKafkaClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);

    private String bootstrapUrl;
    private String username;
    private String password;
    private String securityProtocol;
    private String saslMechanism;
    private String saslJaasConfig;
    private int consumerCreationRetryMs;
    private int producerCreationRetryMs;

    @SuppressWarnings("unused")
    public String getBootstrapUrl() {
        return bootstrapUrl;
    }

    public void setBootstrapUrl(String bootstrapUrl) {
        this.bootstrapUrl = bootstrapUrl;
    }

    @SuppressWarnings("unused")
    public int getConsumerCreationRetryMs() {
        return consumerCreationRetryMs;
    }

    public void setConsumerCreationRetryMs(int consumerCreationRetryMs) {
        this.consumerCreationRetryMs = consumerCreationRetryMs;
    }

    public void setProducerCreationRetryMs(int producerCreationRetryMs) {
        this.producerCreationRetryMs = producerCreationRetryMs;
    }

    @SuppressWarnings("unused")
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @SuppressWarnings("unused")
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @SuppressWarnings("unused")
    public int getProducerCreationRetryMs() {
        return producerCreationRetryMs;
    }

    @SuppressWarnings("unused")
    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    @SuppressWarnings("unused")
    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    @SuppressWarnings("unused")
    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Producer getProducer(Properties props) {
        enrich(props);
        try {
            return (KafkaProducer) super.getProducer(props);
        } catch (KafkaException ke) {
            int retryMs = getProducerCreationRetryMs();
            LOG.warn("KafkaException when trying to create producer. Will wait {}ms before retry.", retryMs);
            sleep(retryMs);
            throw ke;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Consumer getConsumer(Properties props) {
        enrich(props);
        return super.getConsumer(props);
    }

    @Override
    public String getBrokers(KafkaConfiguration configuration) {
        return this.bootstrapUrl;
    }

    private void enrich(Properties props) {
        //
        // Configure Authentication
        //
        if (ObjectHelper.isNotEmpty(securityProtocol)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }

        if (ObjectHelper.isNotEmpty(saslMechanism)) {
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }

        String sjc = saslJaasConfig();

        if (ObjectHelper.isNotEmpty(sjc)) {
            props.put(SaslConfigs.SASL_JAAS_CONFIG, sjc);
        }

    }

    private String saslJaasConfig() {
        if (ObjectHelper.isNotEmpty(saslJaasConfig)) {
            return saslJaasConfig;
        }
        if (ObjectHelper.isEmpty(saslMechanism)) {
            return saslJaasConfig;
        }
        if (ObjectHelper.isEmpty(username) || ObjectHelper.isEmpty(password)) {
            return saslJaasConfig;
        }

        if (saslMechanism.startsWith("PLAIN")) {
            return String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                    this.username,
                    this.password);
        }

        if (saslMechanism.startsWith("SCRAM-")) {
            return String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username='%s' password='%s';",
                    this.username,
                    this.password);
        }

        return saslJaasConfig;
    }

    private void sleep(int retryMs) {
        try {
            Thread.sleep(retryMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Sleep interrupted");
        }
    }

}
