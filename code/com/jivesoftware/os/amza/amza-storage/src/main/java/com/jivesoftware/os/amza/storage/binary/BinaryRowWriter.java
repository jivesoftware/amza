package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.TableRowWriter;
import com.jivesoftware.os.amza.storage.chunks.Filer;
import java.io.IOException;
import java.util.Collection;

public class BinaryRowWriter implements TableRowWriter<byte[]> {

    private final Filer filer;

    public BinaryRowWriter(Filer filer) {
        this.filer = filer;
    }

    @Override
    public void write(Collection<byte[]> rows, boolean append) throws Exception {
        synchronized (filer.lock()) {
            if (append) {
                filer.seek(filer.length());
                writeRows(rows);

            } else {
                filer.seek(filer.length());
                writeRows(rows);
                filer.eof();
            }
        }
    }

    private void writeRows(Collection<byte[]> rows) throws IOException {
        for (byte[] row : rows) {
            filer.write(row.length);
            filer.write(row);
            filer.write(row.length + (2 * 4));
        }
    }
}
