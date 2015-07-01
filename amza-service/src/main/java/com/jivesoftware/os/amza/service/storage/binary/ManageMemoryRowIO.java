package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;

/**
 *
 * @author jonathan.colt
 */
public class ManageMemoryRowIO implements ManageRowIO<MemoryBackedWALFiler> {

    @Override
    public void move(MemoryBackedWALFiler from, MemoryBackedWALFiler to) throws Exception {
        from.seek(0);
        to.seek(0);
        UIO.copy(from, to, 1024);
    }

    @Override
    public void delete(MemoryBackedWALFiler key) throws Exception {
        key.seek(0);
        key.eof();
    }

}
