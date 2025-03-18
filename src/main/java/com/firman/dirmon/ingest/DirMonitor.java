package com.firman.dirmon.ingest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Monitors directory for newly created/copied input file and triggers ingest batch job.
 */
@Service
@EnableAsync
public class DirMonitor {

    private static final Logger LOGGER = Logger.getLogger(DirMonitor.class.getName());
    private final DomainIngestConfig domainIngestConfig;
    /**
     * The workhorse of this class, Java's {@link WatchService}.
     * It gets notified when some file system events happen.
     */
    private final WatchService watchService;
    /**
     * The directory being watched for files being copied/moved to.
     */
    private final Path watchedDir;

    public DirMonitor(DomainIngestConfig domainIngestConfig,
                      @Value("${batch.watchdir}") String watchedDir) throws IOException {
        this.domainIngestConfig = domainIngestConfig;
        this.watchedDir = Paths.get(watchedDir);
        this.watchService = FileSystems.getDefault().newWatchService();
        this.watchedDir.register(watchService, ENTRY_MODIFY, ENTRY_CREATE);
    }

    @Async
    public void processEvents() {
        while (true) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException e) {
                LOGGER.info("Watch service is interrupted");
                return;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent<Path> eventPath = (WatchEvent<Path>) event;
                Path newFile = watchedDir.resolve(eventPath.context());
                LOGGER.info(event.kind() + " : " + newFile.toAbsolutePath());

                // Try getting rw access to the file
                // If no exception is thrown, means we can send it to be ingested
                try (RandomAccessFile access = new RandomAccessFile(newFile.toString(), "rw")) {
                    domainIngestConfig.runJob(newFile.toString());
                } catch (Exception exception) {
                    LOGGER.info( newFile + " has not completed being written");
                }

                if (!key.reset()) {
                    LOGGER.info("Directory " + watchedDir + " is no longer accessible.");
                    break;
                }
            }
        }
    }
}
