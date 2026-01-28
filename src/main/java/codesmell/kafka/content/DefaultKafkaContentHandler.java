package codesmell.kafka.content;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DefaultKafkaContentHandler implements KafkaContentHandler {

    public static final String KEY_BOUNDARY = "--key";
    public static final String HEADER_BOUNDARY = "--header";

    /**
     * Generate a Kafka ProducerRecord from the content passed in
     * 
     * @param content
     * @return
     */
    @Override
    public ProducerRecord<String, String> processContent(String kafkaTopic, String content) {
        ProducerRecord<String, String> record = null;
        if (StringUtils.isNotBlank(kafkaTopic) && StringUtils.isNotBlank(content)) {
            KafkaParts parts = this.splitContentIntoParts(content);

            String key = parts.key;
            List<Header> headers = parts.headers;
            String bodyContent = parts.body;

            record = this.generateRecord(kafkaTopic, key, headers, bodyContent);
        }
        return record;
    }

    // TODO: consider using pattern matching and switch expressions (JDK 17+)
    // as well as Optional to reduce null checks
    protected KafkaParts splitContentIntoParts(String content) {
        KafkaParts parts = null;

        if (!Objects.isNull(content) && StringUtils.isNotBlank(content)) {
            // we have content
            parts = new KafkaParts();

            // key
            String headerContent = this.splitOutKey(parts, content);

            // headers
            this.splitOutHeaders(parts, headerContent);
        }

        return parts;
    }

    private String splitOutKey(KafkaParts parts, String content) {
        String headerContent = null;
        String[] keySplitContent = content.split(KEY_BOUNDARY, 2);
        if (keySplitContent.length > 1) {
            String key = keySplitContent[0].trim();
            // Convert empty string to null
            parts.key = key.isEmpty() ? null : key;
            headerContent = keySplitContent[1].stripLeading();
        } else {
            headerContent = content;
        }
        return headerContent;
    }

    private void splitOutHeaders(KafkaParts parts, String content) {
        // headers
        String[] splitContent = content.split(HEADER_BOUNDARY, 2);
        if (splitContent.length > 1) {
            parts.headers = this.buildHeaders(splitContent[0]);
            parts.body = splitContent[1].stripLeading();
        } else {
            parts.body = splitContent[0].stripLeading();
        }
    }
    /**
     * each line is a key value pair
     * 
     * @param headerContent
     * @return
     */
    private List<Header> buildHeaders(String headerContent) {
        List<Header> headers = null;

        if (!Objects.isNull(headerContent) && StringUtils.isNotBlank(headerContent)) {
            String[] splitHeaders = headerContent.split("\n");
            if (ArrayUtils.isNotEmpty(splitHeaders)) {

                headers = new ArrayList<>();

                for (String header : splitHeaders) {
                    // handle empty header lines
                    header = header.trim();
                    if (header.isEmpty())
                        continue;

                    String[] splitKeyValue = header.split(":");
                    if (splitKeyValue.length == 2) {
                        String key = splitKeyValue[0];
                        String value = splitKeyValue[1].stripTrailing();
                        headers.add(new RecordHeader(key, value.getBytes()));
                    } else {
                        // invalid header
                        throw new IllegalArgumentException("Malformed header - expected 'key:value' format, got: '" + header + "'");
                    }
                }
            }
        }

        return headers;
    }

    private ProducerRecord<String, String> generateRecord(String topic,
            String key, List<Header> headers, String bodyContents) {

        Integer partition = null; // any partition will be fine
        Long timestamp = null; // let broker assign timestamp
        return new ProducerRecord<>(topic, partition, timestamp, key, bodyContents, headers);
    }

    private class KafkaParts {
        String key;
        List<Header> headers;
        String body;
    }
}