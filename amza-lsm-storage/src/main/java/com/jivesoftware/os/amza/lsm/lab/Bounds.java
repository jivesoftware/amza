package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
class Bounds {

    private final byte[] fps;

    public Bounds(byte[] fps) {
        this.fps = fps;
    }

    private static Bounds read(IReadable readable, byte[] lengthBuffer) throws IOException {
        byte[] fps = UIO.readByteArray(readable, "fps", lengthBuffer);
        return new Bounds(fps);
    }

}
