package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TableRowReader.Stream;
import com.jivesoftware.os.amza.storage.chunks.Filer;

public class BinaryRowReader implements TableRowReader<byte[]> {

    private final Filer filer;

    public BinaryRowReader(Filer filer) {
        this.filer = filer;
    }

    @Override
    public void read(boolean reverse, Stream<byte[]> stream) throws Exception {

        if (reverse) {
            synchronized (filer.lock()) {
                long seekTo = filer.length() - 4;
                while (seekTo > -4) {
                    if (seekTo < 0) {
                        filer.seek(0);
                    } else {
                        filer.seek(seekTo);
                        int priorLength = filer.read();
                        int length = filer.read();
                        byte[] row = new byte[length];
                        filer.readFully(row);
                        if (!stream.stream(row)) {
                            break;
                        }
                        seekTo = priorLength;
                    }
                }
            }
        } else {
            synchronized (filer.lock()) {
                filer.seek(0);
                int length = filer.read();
                while (length != -1) {
                    byte[] row = new byte[length];
                    filer.readFully(row);
                    if (!stream.stream(row)) {
                        break;
                    }
                    filer.skipBytes(4);
                    length = filer.read();
                }
            }
        }
    }
}