/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ServerContext {

    private final AtomicBoolean running = new AtomicBoolean();

    public boolean start() {
        return running.compareAndSet(false, true);
    }

    public boolean stop() {
        return running.compareAndSet(true, false);
    }

    public boolean running() {
        return running.get();
    }

    public void closeAndCatch(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ex) {
            }
        }
    }
}
