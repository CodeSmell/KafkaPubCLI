package codesmell.kafka.content;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaContentHandler {

    ProducerRecord<String, String> processContent(String kafkaTopic, String content);
}
