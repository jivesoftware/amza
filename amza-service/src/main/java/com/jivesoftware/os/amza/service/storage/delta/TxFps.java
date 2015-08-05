package com.jivesoftware.os.amza.service.storage.delta;

/**
 *
 */
class TxFps {
    final byte[] prefix;
    final long txId;
    long[] fps;

    public TxFps(byte[] prefix, long txId, long[] fps) {
        this.prefix = prefix;
        this.txId = txId;
        this.fps = fps;
    }
}
