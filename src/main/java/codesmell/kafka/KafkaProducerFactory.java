package codesmell.kafka;

import codesmell.main.ProducerArgs;
import org.apache.kafka.clients.producer.KafkaProducer;

@FunctionalInterface
public interface KafkaProducerFactory {

    KafkaProducer<String, String> buildProducer(ProducerArgs args);
}
