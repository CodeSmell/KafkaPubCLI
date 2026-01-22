package codesmell.file;

import java.util.function.Predicate;

public interface DirectoryPollingService {
    /**
     * Poll the given directory for files, process each file with the provided
     * processor and optionally delete files upon successful processing.
     * 
     * @param directory
     * @param fileProcessor
     * @param deleteOnSuccess
     */
    void pollDirectory(String directory, Predicate<String> fileProcessor, boolean deleteOnSuccess);

    /**
     * Poll the given directory for files, process each file with the provided
     * processor without deleting files upon successful processing.
     * 
     * @param directory
     * @param fileProcessor
     */
    default void pollDirectory(String directory, Predicate<String> fileProcessor) {
        pollDirectory(directory, fileProcessor, false);
    }
}
