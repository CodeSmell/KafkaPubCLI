package codesmell.file;

import java.nio.file.DirectoryStream;
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
        Path messageDirectory = Paths.get(messageLocation);

        // fail fast if not a directory
        if (!Files.exists(messageDirectory) || !Files.isDirectory(messageDirectory)) {
            throw new IllegalArgumentException("file location is not a valid directory: " + messageLocation);
        } else {
            try (DirectoryStream<Path> filesStream = Files.newDirectoryStream(messageDirectory)) {
                boolean foundFiles = false;
                for (Path filePath : filesStream) {
                    // skip sub-directories and symbolic links
                    if (Files.isRegularFile(filePath)){
                        foundFiles = true;
                        this.processFile(filePath, processor, deleteOnSuccess);
                    }
                }
                if (!foundFiles) {
                    LOGGER.info("no files found...");
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to poll directory: " + messageLocation, e);
            }
        }
    }

    private String readFileContents(Path theFile) {
        try {
            return Files.readString(theFile);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read file: " + theFile.getFileName(), e);
        }
    }

    private void processFile(Path file, Predicate<String> processor, boolean deleteOnSuccess) {
        LOGGER.info("processing file: {}", file.getFileName());
        String fileContents = this.readFileContents(file);
        boolean successfullyProcessedFile = processor.test(fileContents);
        
        if (successfullyProcessedFile) {
            LOGGER.info("file processed successfully: {}", file.getFileName());

            if (deleteOnSuccess) {
                this.deleteFile(file);
            } else {
                LOGGER.info("based on config will not be deleting files!");
            }
        } else {
            LOGGER.warn("failed to process file: {}", file.getFileName());
            // TODO: should allow break and stop
            // as well as keep trying options      
        }
    }

    private void deleteFile(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete file: " + file.getFileName(), e);
        }
    }

}
