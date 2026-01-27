package codesmell.kafka;

import codesmell.file.DefaultDirectoryPollingService;
import codesmell.kafka.content.DefaultContentHandler;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMain.class);

    /**
     * main
     */
    public static final void main(String[] args) {
        ProducerArgs cliArgs = new ProducerArgs();
        
        JCommander jcomm = JCommander.newBuilder()
                .addObject(cliArgs)
                .build();

        try {
            jcomm.parse(args);

            if (cliArgs.isHelp()) {
                jcomm.usage();
            } else {
                LOGGER.info(">>>>> starting up...");
                KafkaMain.doKafkaPublish(cliArgs);
            }

        } catch (ParameterException e) {
            LOGGER.error("incorrect usage - try using -help");
        }

    }
    
    /**
     * publish all of the files in a given directory
     */
    public static void doKafkaPublish(ProducerArgs cliArgs) {
        // try-with-resources to ensure proper close
        try (DefaultKafkaProducerUtil util = new DefaultKafkaProducerUtil(
                cliArgs, 
                new DefaultContentHandler(),
                new DefaultDirectoryPollingService())) {
            
            // Register shutdown hook to gracefully 
            // close the producer on Ctrl+C or shutdown
            final DefaultKafkaProducerUtil utilRef = util;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    utilRef.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing producer on shutdown", e);
                }
            }));
            
            // this will run until interrupted (Ctrl+C)
            util.processMessagesInDirectory(cliArgs.getMessageLocation());
            
        } catch (Exception e) {
            LOGGER.error("Fatal error: failed to process messages from {}", cliArgs.getMessageLocation(), e);
        }
    }
}
