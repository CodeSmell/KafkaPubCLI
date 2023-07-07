package codesmell.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerFactory {

    /**
     * get a Kafka producer based on args passed in
     */
    public static KafkaProducer<String, String> buildKafkaProducer(ProducerArgs args) {
        Properties config = new Properties();
        config.put("client.id", args.getClientId());
        config.put("bootstrap.servers", args.getBootstrap());
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", args.getAckMode());
        config.put("retries", args.getRetries());
        config.put("retry.backoff.ms", args.getRetryDelay());
        config.put("max.in.flight.requests.per.connection", args.getMaxInflight());
        config.put("batch.size", args.getBatchSizeBytes());
        config.put("linger.ms", args.getBatchDelay());
        
        // if arguments are marked to use a secure connection to the broker
        // then the other arguments will be used
        // otherwise they are ignored
        if (args.isSecure()) {
            config.put("security.protocol", args.getSecurityProtocol());
            config.put("sasl.mechanism", args.getSaslMechanism());
            config.put("sasl.jaas.config", args.getSaslJaasConfig());
            config.put("ssl.truststore.type", args.getTrustStoreType());
            config.put("ssl.truststore.location", args.getTruststoreLocation());
            config.put("ssl.truststore.password", args.getTruststorePassword());
        }

        return new KafkaProducer<>(config);
    }
}
