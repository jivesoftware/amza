package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TableRowReader.Stream;
import com.jivesoftware.os.amza.storage.chunks.IFiler;
import com.jivesoftware.os.amza.storage.chunks.UIO;

public class BinaryRowReader implements TableRowReader<byte[]> {

    private final IFiler filer;

    public BinaryRowReader(IFiler filer) {
        this.filer = filer;
    }

    @Override
    public void read(boolean reverse, Stream<byte[]> stream) throws Exception {

        if (reverse) {
            synchronized (filer.lock()) {
                long seekTo = filer.length() - 4; // last length int
                if (seekTo < 0) {
                    return;
                }
                while (seekTo >= 0) {
                    filer.seek(seekTo);
                    int priorLength = UIO.readInt(filer, "priorLength");
                    seekTo -= (priorLength + 4);
                    filer.seek(seekTo);

                    int length = UIO.readInt(filer, "length");
                    byte[] row = new byte[length];
                    filer.read(row);
                    if (!stream.stream(row)) {
                        break;
                    }

                    seekTo -= 4;
                }
            }
        } else {
            synchronized (filer.lock()) {
                if (filer.length() == 0) {
                    return;
                }
                filer.seek(0);
                while (filer.getFilePointer() < filer.length()) {
                    int length = UIO.readInt(filer, "length");
                    byte[] row = new byte[length];
                    if (length > 0) {
                        filer.read(row);
                    }
                    if (!stream.stream(row)) {
                        break;
                    }
                    UIO.readInt(filer, "length");
                }
            }
        }
    }
}
