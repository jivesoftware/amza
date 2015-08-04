package com.jivesoftware.os.amza.service.storage.delta;

/**
 *
 */
class TxFps {
    final long txId;
    long[] fps;

    public TxFps(long txId, long[] fps) {
        this.txId = txId;
        this.fps = fps;
    }
}
