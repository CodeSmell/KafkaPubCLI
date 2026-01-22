package codesmell.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of DirectoryPollingService
 * Given a directory path, it will poll for files in that directory
 */
public class DefaultDirectoryPollingService implements DirectoryPollingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDirectoryPollingService.class);

    @Override
    public void pollDirectory(String messageLocation, Predicate<String> processor, boolean deleteOnSuccess) {
        File messageDirectory = this.findMessageLocationAsFile(messageLocation);
        File[] files = messageDirectory.listFiles();

        if (files == null || files.length == 0) {
            LOGGER.info("no files found...");
        } else {
            for (File file : files) {
                this.processFile(file, processor, deleteOnSuccess);
            }
        }
    }

    private File findMessageLocationAsFile(String messageLocation) {
        return new File(messageLocation);
    }

    private String readFileContents(File theFile) {
        try {
            Path path = Paths.get(theFile.getCanonicalPath());
            return Files.readString(path);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void processFile(File file, Predicate<String> processor, boolean deleteOnSuccess) {
        LOGGER.info("processing file: {}", file.getName());
        String fileContents = this.readFileContents(file);
        boolean successullyProcessedFile = processor.test(fileContents);
        
        if (successullyProcessedFile) {
            LOGGER.info("file processed successfully: {}", file.getName());

            if (deleteOnSuccess) {
                file.delete();
            } else {
                LOGGER.info("based on config will not be deleting files!");
            }
        } else {
            LOGGER.warn("failed to process file: {}", file.getName());
            // TODO: should allow break and stop
            // as well as keep trying options      
        }
    }

}
