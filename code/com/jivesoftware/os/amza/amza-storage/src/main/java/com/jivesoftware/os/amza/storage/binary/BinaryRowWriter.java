package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.TableRowWriter;
import com.jivesoftware.os.amza.storage.chunks.IFiler;
import com.jivesoftware.os.amza.storage.chunks.UIO;
import java.io.IOException;
import java.util.Collection;

public class BinaryRowWriter implements TableRowWriter<byte[]> {

    private final IFiler filer;

    public BinaryRowWriter(IFiler filer) {
        this.filer = filer;
    }

    @Override
    public void write(Collection<byte[]> rows, boolean append) throws Exception {
        synchronized (filer.lock()) {
            if (append) {
                filer.seek(filer.length()); // seek to end of file.
                writeRows(rows);
            } else {
                filer.seek(0);
                writeRows(rows);
                filer.eof(); // trim file to size.
            }
            filer.flush();
        }
    }

    private void writeRows(Collection<byte[]> rows) throws IOException {
        for (byte[] row : rows) {
            UIO.writeInt(filer, row.length, "length");
            filer.write(row);
            UIO.writeInt(filer, row.length, "length");
        }
    }
}
