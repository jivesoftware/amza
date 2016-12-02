package com.jivesoftware.os.amza.service;

/**
 *
 */
public interface AmzaSystemReady {
    void await(long timeoutInMillis) throws Exception;
}
