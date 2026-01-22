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
        DefaultKafkaProducerUtil util = new DefaultKafkaProducerUtil(
            cliArgs, 
            new DefaultContentHandler(),
            new DefaultDirectoryPollingService());
        util.processMessagesInDirectory(cliArgs.getMessageLocation());
    }
    
}
