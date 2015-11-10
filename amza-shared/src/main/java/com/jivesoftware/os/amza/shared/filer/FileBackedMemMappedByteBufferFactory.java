package com.jivesoftware.os.amza.shared.filer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.commons.io.Charsets;

/**
 * TODO this implementation of ByteBufferFactory is inherently unsafe because its allocate() method is only capable of growing an existing buffer rather than
 * handing out a new one. Eventually we need to extend ByteBufferFactory to formalize notions of create(), open(), copy(), resize().
 *
 * @author jonathan.colt
 */
public class FileBackedMemMappedByteBufferFactory implements com.jivesoftware.os.filer.io.ByteBufferFactory {

    private final String prefix;
    private final int directoryOffset;
    private final File[] directories;

    public FileBackedMemMappedByteBufferFactory(String prefix, int directoryOffset, File... directories) {
        this.prefix = prefix;
        this.directoryOffset = directoryOffset;
        this.directories = directories;
    }

    private File getDirectory(String key) {
        return directories[Math.abs((key.hashCode() + directoryOffset) % directories.length)];
    }

    public MappedByteBuffer open(String key) {
        try {
            //System.out.println(String.format("Open key=%s for directories=%s", key, Arrays.toString(directories)));
            File directory = getDirectory(key);
            ensureDirectory(directory);
            File file = new File(directory, prefix + "-" + key);
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.seek(0);
                try (FileChannel channel = raf.getChannel()) {
                    return channel.map(FileChannel.MapMode.READ_WRITE, 0, (int) channel.size());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists(byte[] key) {
        String name = new String(key, Charsets.UTF_8);
        File directory = getDirectory(name);
        if (directory.exists()) {
            return new File(directory, prefix + "-" + name).exists();
        }
        return false;
    }

    @Override
    public ByteBuffer allocate(byte[] key, long length) {
        try {
            //System.out.println(String.format("Allocate key=%s length=%s for directories=%s", key, length, Arrays.toString(directories)));
            String name = new String(key, Charsets.UTF_8);
            File directory = getDirectory(name);
            ensureDirectory(directory);
            File file = new File(directory, prefix + "-" + name);
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                if (length > raf.length()) {
                    raf.seek(length - 1);
                    raf.write(0);
                }
                raf.seek(0);
                try (FileChannel channel = raf.getChannel()) {
                    return channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer reallocate(byte[] key, ByteBuffer oldBuffer, long newSize) {
        //System.out.println(String.format("Reallocate key=%s newSize=%s for directories=%s", key, newSize, Arrays.toString(directories)));
        return allocate(key, newSize);
    }

    private void ensureDirectory(File directory) {
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                if (!directory.exists()) {
                    throw new RuntimeException("Failed to create directory: " + directory);
                }
            }
        }
    }

}