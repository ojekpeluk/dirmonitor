package com.firman.dirmon.monitor;

import com.firman.dirmon.ingest.IngestFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.nio.file.Paths;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DirMonitorServiceTest {

    @Test
    public void testProcessEvents(@TempDir File watchedDir) throws Exception {
        IngestFile ingestFile = Mockito.mock(IngestFile.class);
        DirectoryMonitor monitor = new DirMonitorService(ingestFile, watchedDir.getAbsolutePath());
        Thread dirMonitorThread = new Thread(monitor::processEvents);
        dirMonitorThread.start();
        File newFile = Paths.get(watchedDir.getAbsolutePath(), "newFile").toFile();
        newFile.createNewFile();
        FileCopyUtils.copy(newFile, Paths.get(watchedDir.getAbsolutePath(), "newFile").toFile());
        // The tricky part is sometimes it takes longer for the Java app to get filesystem notification
        // The day the event takes longer than 10 sec to register is the day this test fails ):
        Thread.sleep(10000);
        verify(ingestFile, times(1)).ingestFile(Paths.get(watchedDir.getAbsolutePath(), "newFile").toString());
        dirMonitorThread.interrupt();
    }

    @Test
    public void testProcessEventsInterrupted(@TempDir File watchedDir) throws Exception {
        IngestFile ingestFile = Mockito.mock(IngestFile.class);
        DirMonitorService dirMonitor = new DirMonitorService(ingestFile, watchedDir.getAbsolutePath());
        Thread dirMonitorThread = new Thread(dirMonitor::processEvents);
        dirMonitorThread.start();
        // Calling interrupt on the thread will result in processEvents() to return
        dirMonitorThread.interrupt();
        File newFile = Paths.get(watchedDir.getAbsolutePath(), "newFile").toFile();
        newFile.createNewFile();
        FileCopyUtils.copy(newFile, Paths.get(watchedDir.getAbsolutePath(), "newFile").toFile());
        Thread.sleep(10000);
        // No event should be registered
        verify(ingestFile, times(0)).ingestFile(Paths.get(watchedDir.getAbsolutePath(), "newFile").toString());
    }
}
