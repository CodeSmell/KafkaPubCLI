package codesmell.kafka;

import codesmell.kafka.content.ContentHandler;
import codesmell.kafka.content.DefaultContentHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DefaultKafkaProducerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaProducerUtil.class);

    private final ProducerArgs args;
    private final KafkaProducer<String, String> producer;
    private final ContentHandler contentHandler;

    public DefaultKafkaProducerUtil(ProducerArgs args, DefaultContentHandler contentHandler) {
        this.args = args;
        this.producer = KafkaProducerFactory.buildKafkaProducer(args);
        this.contentHandler = contentHandler;
    }

    /**
     * scan the directory for files
     * publish each file as a Kafka message
     */
    public void processMessagesInDirectory(String messageLocation) {
        boolean keepRunning = true;

        File messageDirectory = this.findMessageLocationAsFile(messageLocation);

        while (keepRunning) {
            LOGGER.info("looking for files in " + messageLocation);
            File[] files = this.pollDirectory(messageDirectory);

            if (files == null || files.length == 0) {
                LOGGER.info("no files found...");
            } else {
                for (File file : files) {
                    this.processFile(file);
                }
            }

            keepRunning = this.keepRunningWithDelay();
        }
    }

    private void processFile(File file) {
        LOGGER.info("processing a file - " + file.getName());
        String fileContents = this.readFileContents(file);

        ProducerRecord<String, String> record = contentHandler.processContent(args.getTopic(), fileContents);
        boolean isProcessedSuccessfully = this.sendRecord(producer, record);

        if (isProcessedSuccessfully) {
            // sent successfully
            // so remove it (based on config)
            if (args.isNoDeleteFiles()) {
                LOGGER.info("based on config will not be deleting files!");
            } else {
                file.delete();
            }
        } else {
            LOGGER.info("could not publish message...");
            // TODO: should allow break and stop
            // as well as keep trying options
        }

        producer.flush();
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

    private File findMessageLocationAsFile(String messageLocation) {
        return new File(messageLocation);
    }

    private File[] pollDirectory(File messageLocation) {
        return messageLocation.listFiles();
    }

    private String readFileContents(File theFile) {
        try {
            Path path = Paths.get(theFile.getCanonicalPath());
            return Files.readString(path);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
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

}
