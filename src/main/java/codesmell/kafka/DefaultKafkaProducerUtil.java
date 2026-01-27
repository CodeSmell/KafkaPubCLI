package codesmell.kafka;

import codesmell.file.DirectoryPollingService;
import codesmell.kafka.content.ContentHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultKafkaProducerUtil implements AutoCloseable{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaProducerUtil.class);

    private final ProducerArgs args;
    private final KafkaProducer<String, String> producer;
    private final ContentHandler contentHandler;
    private final DirectoryPollingService directoryPollingService;

    public DefaultKafkaProducerUtil(ProducerArgs args,
            ContentHandler contentHandler,
            DirectoryPollingService directoryPollingService) {
        this.args = args;
        this.producer = KafkaProducerFactory.buildKafkaProducer(args);
        this.contentHandler = contentHandler;
        this.directoryPollingService = directoryPollingService;
    }

    /**
     * scan the directory for files
     * publish each file as a Kafka message
     */
    public void processMessagesInDirectory(String messageLocation) {
        boolean keepRunning = true;

        while (keepRunning) {
            LOGGER.info("looking for files in " + messageLocation);

            boolean shouldDeleteFiles = !args.isNoDeleteFiles();
            directoryPollingService.pollDirectory(messageLocation, this::processFileContents, shouldDeleteFiles);

            keepRunning = this.keepRunningWithDelay();
        }
    }

    private boolean keepRunningWithDelay() {
        boolean keepRunning = true;

        if (args.isRunOnce()) {
            keepRunning = false;
        } else {
            try {
                Thread.sleep(args.getDelaySeconds());
            } catch (InterruptedException e) {
                // ignoring
            }
        }

        return keepRunning;
    }

    /**
     * Used as Predicate for processing files
     * The contents are published to Kafka.
     * Returns true if the message was published successfully, false otherwise.
     */
    private boolean processFileContents(String fileContents) {
        ProducerRecord<String, String> record = contentHandler.processContent(args.getTopic(), fileContents);
        boolean isProcessedSuccessfully = this.sendRecord(producer, record);

        if (isProcessedSuccessfully) {
            LOGGER.info("message published successfully");
        } else {
            LOGGER.info("could not publish message...");
        }

        producer.flush();

        return isProcessedSuccessfully;
    }

    private boolean sendRecord(KafkaProducer<String, String> kp, ProducerRecord<String, String> record) {
        boolean sentRecord = false;

        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            sentRecord = true;

            LOGGER.info("wrote {} to partition {} at offset {}", record.value(),
                    recordMetadata.partition(),
                    recordMetadata.offset());

        } catch (Exception e) {
            e.printStackTrace();
        }

        return sentRecord;
    }

    /**
     * AutoCloseable implementation that closes the Kafka producer
     * this allows use in try-with-resources blocks
     */
    @Override
    public void close() throws Exception {
        if (producer != null) {
            LOGGER.info("closing Kafka producer...");
            producer.close();
        }
    }

}
