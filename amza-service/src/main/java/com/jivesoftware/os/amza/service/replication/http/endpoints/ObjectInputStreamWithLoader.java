package com.jivesoftware.os.amza.service.replication.http.endpoints;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.StreamCorruptedException;

/**
 *
 */
public class ObjectInputStreamWithLoader extends ObjectInputStream {
    private final ClassLoader loader;

    public ObjectInputStreamWithLoader(InputStream in, ClassLoader loader) throws IOException {
        super(in);
        if (loader == null) {
            throw new IllegalArgumentException("Illegal null argument to ObjectInputStreamWithLoader");
        }
        this.loader = loader;
    }

    protected Class resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
        String name = classDesc.getName();
        try {
            return Class.forName(name, false, loader);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }
}
