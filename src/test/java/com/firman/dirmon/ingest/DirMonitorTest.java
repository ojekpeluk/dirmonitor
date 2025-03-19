package com.firman.dirmon.ingest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.nio.file.Paths;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DirMonitorTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();
    private final DomainIngestConfig domainIngestConfig = Mockito.mock(DomainIngestConfig.class);

    @Test
    public void testProcessEvents() throws Exception {
        File watchedDir = tmp.newFolder();
        DirMonitor dirMonitor = new DirMonitor(domainIngestConfig, watchedDir.getAbsolutePath());
        Thread dirMonitorThread = new Thread(dirMonitor::processEvents);
        dirMonitorThread.start();
        File newFile = tmp.newFile("newFile");
        FileCopyUtils.copy(newFile, Paths.get(watchedDir.getAbsolutePath(), "newFile").toFile());
        // The tricky part is sometimes it takes longer for the Java app to get filesystem notification
        // The day the event takes longer than 10 sec to register is the day this test fails ):
        Thread.sleep(10000);
        verify(domainIngestConfig, times(1)).runJob(Paths.get(watchedDir.getAbsolutePath(), "newFile").toString());
        dirMonitorThread.interrupt();
    }

    @Test
    public void testProcessEventsInterrupted() throws Exception {
        File watchedDir = tmp.newFolder();
        DirMonitor dirMonitor = new DirMonitor(domainIngestConfig, watchedDir.getAbsolutePath());
        Thread dirMonitorThread = new Thread(dirMonitor::processEvents);
        dirMonitorThread.start();
        // Calling interrupt on the thread will result in processEvents() to return
        dirMonitorThread.interrupt();
        File newFile = tmp.newFile("newFile");
        FileCopyUtils.copy(newFile, Paths.get(watchedDir.getAbsolutePath(), "newFile").toFile());
        Thread.sleep(10000);
        // No event should be registered
        verify(domainIngestConfig, times(0));
    }


}
