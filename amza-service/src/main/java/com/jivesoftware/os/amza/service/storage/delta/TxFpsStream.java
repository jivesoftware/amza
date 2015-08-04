package com.jivesoftware.os.amza.service.storage.delta;

/**
 *
 */
interface TxFpsStream {
    boolean stream(TxFps txFps) throws Exception;
}
