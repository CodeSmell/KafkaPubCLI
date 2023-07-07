package codesmell.kafka.content;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface ContentHandler {

    ProducerRecord<String, String> processContent(String topic, String content);
}
