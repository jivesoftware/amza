package com.jivesoftware.os.amza.service.replication.http.endpoints;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 *
 */
public class FilterClassLoader extends ClassLoader {

    public void defineClass(String fullClassName, byte[] data) throws IOException {
        defineClass(fullClassName, data, 0, data.length);
    }
}
