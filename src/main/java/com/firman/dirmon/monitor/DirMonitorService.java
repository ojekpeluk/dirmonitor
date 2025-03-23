package com.firman.dirmon.monitor;

import com.firman.dirmon.ingest.IngestFile;
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
 *
 * @author Firman
 */
@Service
@EnableAsync
public class DirMonitorService implements DirectoryMonitor {

    private static final Logger LOGGER = Logger.getLogger(DirMonitorService.class.getName());
    private final IngestFile ingestFile;
    /**
     * The workhorse of this class, Java's {@link WatchService}.
     * It gets notified when some file system events happen.
     */
    private final WatchService watchService;
    /**
     * The directory being watched for files being copied/moved to.
     */
    private final Path watchedDir;

    public DirMonitorService(IngestFile ingestFile,
                             @Value("${batch.watchdir}") String watchedDir) throws IOException {
        this.ingestFile = ingestFile;
        this.watchedDir = Paths.get(watchedDir);
        if (!this.watchedDir.toFile().exists()
                && !this.watchedDir.toFile().mkdir()) {
            throw new RuntimeException(this.watchedDir.toAbsolutePath() + " does not exist and can't be created!");
        } else {
            LOGGER.info("Watched dir: " + this.watchedDir.toAbsolutePath());
        }
        this.watchService = FileSystems.getDefault().newWatchService();
        this.watchedDir.register(watchService, ENTRY_MODIFY, ENTRY_CREATE);
    }

    @Async
    @Override
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
                @SuppressWarnings("unchecked")
                WatchEvent<Path> eventPath = (WatchEvent<Path>) event;
                Path newFile = watchedDir.resolve(eventPath.context());
                LOGGER.info(event.kind() + " : " + newFile.toAbsolutePath());

                // Try getting rw access to the file
                // If no exception is thrown, means we can send it to be ingested
                try (RandomAccessFile access = new RandomAccessFile(newFile.toString(), "rw")) {
                    ingestFile.ingestFile(newFile.toString());
                } catch (SecurityException | IOException exception) {
                    LOGGER.info(newFile + " has not completed being written");
                } catch (Exception exception) {
                    LOGGER.info("Failed ingesting file: " + exception.getLocalizedMessage());
                    break;
                }

                if (!key.reset()) {
                    LOGGER.info("Directory " + watchedDir + " is no longer accessible.");
                    break;
                }
            }
        }
    }
}
