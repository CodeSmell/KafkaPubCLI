package codesmell.kafka;

import codesmell.file.DirectoryPollingService;
import codesmell.kafka.content.DefaultKafkaContentHandler;
import codesmell.kafka.content.KafkaContentHandler;
import codesmell.main.ProducerArgs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultKafkaProducerUtilTest {

    private DefaultKafkaProducerUtil producerUtil;
    private ProducerArgs args;
    private KafkaContentHandler contentHandler;
    private DirectoryPollingService mockDirectoryPollingService;
    private KafkaProducerFactory mockKafkaFactory;
    private KafkaProducer<String, String> mockProducer;

    @BeforeEach
    void setUp() {
        args = this.createTestProducerArgs();
        contentHandler = new DefaultKafkaContentHandler();
        mockDirectoryPollingService = mock(DirectoryPollingService.class);
        // set up mock KafkaProducer that can be returned by the factory
        mockProducer = mock(KafkaProducer.class);
        mockKafkaFactory = mock(KafkaProducerFactory.class);
        when(mockKafkaFactory.buildProducer(args)).thenReturn(mockProducer);
    }

    @Test
    void test_processMessagesInDirectory_runOnce() {
        producerUtil = new DefaultKafkaProducerUtil(args, contentHandler, mockDirectoryPollingService,
                mockKafkaFactory);
        String messageLocation = "/test/path";

        this.mockKafkaProducerSend();
        producerUtil.processMessagesInDirectory(messageLocation);

        // capture the Predicate used to process file contents
        ArgumentCaptor<Predicate<String>> processorCaptor = ArgumentCaptor.forClass(Predicate.class);
        verify(mockDirectoryPollingService).pollDirectory(eq(messageLocation), processorCaptor.capture(), anyBoolean());

        // invoke the Predicate to simulate processing a file
        // since we mocked the polling service and that would not happen otherwise
        boolean processed = processorCaptor.getValue().test("test message");
        assertTrue(processed);

        // capture the ProducerRecord created by KafkaContentHander and sent to
        // KafkaProducer
        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(recordCaptor.capture());
        assertTrue(recordCaptor.getValue().value().contains("test message"));

        // verify pollDirectory was only called once (runOnce is true by default)
        verify(mockDirectoryPollingService, times(1)).pollDirectory(eq(messageLocation), any(Predicate.class),
                anyBoolean());
    }

    @Test
    void test_processMessagesInDirectory_runMultiple() {
        args = this.setFieldValue(args, "runOnce", false);
        producerUtil = new DefaultKafkaProducerUtil(args, contentHandler, mockDirectoryPollingService,
                mockKafkaFactory);
        String messageLocation = "/test/path";

        this.mockKafkaProducerSend();

        // Use a separate thread to interrupt after allowing a couple of poll cycles
        Thread testThread = Thread.currentThread();
        Thread interrupter = new Thread(() -> {
            try {
                // Wait long enough for at least 2 poll cycles
                Thread.sleep(220);
                testThread.interrupt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        interrupter.start();
        producerUtil.processMessagesInDirectory(messageLocation);

        try {
            interrupter.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // verify pollDirectory was called more than once (runOnce is false)
        verify(mockDirectoryPollingService, atLeast(2)).pollDirectory(eq(messageLocation), 
            any(Predicate.class), anyBoolean());
    }

    private void mockKafkaProducerSend() {
        // KafkaProducer.send will return a Future<RecordMetadata>
        // and we want to simulate successful send
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(42L);
        when(mockProducer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));
    }

    private ProducerArgs createTestProducerArgs() {
        // Since ProducerArgs uses @Parameter annotations for CLI parsing,
        // we use reflection to set values for testing
        ProducerArgs testArgs = new ProducerArgs();
        testArgs = setFieldValue(testArgs, "topic", "test-topic");
        testArgs = setFieldValue(testArgs, "bootstrap", "bootstrap");
        testArgs = setFieldValue(testArgs, "ackMode", "1");
        testArgs = setFieldValue(testArgs, "runOnce", true);
        testArgs = setFieldValue(testArgs, "noDeleteFiles", false);
        testArgs = setFieldValue(testArgs, "delayMillis", 100);
        testArgs = setFieldValue(testArgs, "clientId", "test-client");
        testArgs = setFieldValue(testArgs, "retries", 0);
        testArgs = setFieldValue(testArgs, "retryDelay", 100);
        testArgs = setFieldValue(testArgs, "maxInflight", 1);
        testArgs = setFieldValue(testArgs, "batchSizeBytes", 16384);
        testArgs = setFieldValue(testArgs, "batchDelay", 0);
        testArgs = setFieldValue(testArgs, "isSecure", false);
        testArgs = setFieldValue(testArgs, "messageLocation", "/test/path");
        return testArgs;
    }

    private ProducerArgs setFieldValue(ProducerArgs args, String fieldName, Object value) {
        try {
            var field = ProducerArgs.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(args, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
        }
        return args;
    }
}
