package com.firman.dirmon.monitor;

/**
 * Interface implemented by @Service that receives filesystem events and processes them.
 * @author Firman
 */
public interface DirectoryMonitor {
    /**
     * Process file system events.
     */
    public void processEvents();
}
