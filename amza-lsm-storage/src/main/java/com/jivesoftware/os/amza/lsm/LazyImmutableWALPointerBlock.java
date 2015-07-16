package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import java.lang.ref.WeakReference;

/**
 *
 * @author jonathan.colt
 */
public class LazyImmutableWALPointerBlock {

    private final long fp;
    private WeakReference<ImmutableWALPointerBlock> weakIndex;

    public LazyImmutableWALPointerBlock(long fp) {
        this.fp = fp;
    }

    private ImmutableWALPointerBlock hydrateAsNeeded(ImmutableWALPointerBlockHydrator hydrator) {
        ImmutableWALPointerBlock index = null;
        if (weakIndex != null) {
            index = weakIndex.get();
        }
        if (index == null || weakIndex == null) {
            index = hydrator.hydrate(fp);
            weakIndex = new WeakReference<>(index);
        }
        return index;
    }

    public boolean getPointer(ImmutableWALPointerBlockHydrator hydrator, byte[] key, WALKeyPointerStream stream) throws Exception {
        return hydrateAsNeeded(hydrator).getPointer(key, stream);
    }

    // expects walKeys to be in lex order.
    public boolean getPointers(ImmutableWALPointerBlockHydrator hydrator, WALKeys walKeys, WALKeyPointerStream stream) throws Exception {
        return hydrateAsNeeded(hydrator).getPointers(walKeys, stream);
    }

    public boolean getPointers(ImmutableWALPointerBlockHydrator hydrator, KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        return hydrateAsNeeded(hydrator).getPointers(keyValues, stream);
    }

    public boolean containsKeys(ImmutableWALPointerBlockHydrator hydrator, WALKeys walKeys, KeyContainedStream stream) throws Exception {
        return hydrateAsNeeded(hydrator).containsKeys(walKeys, stream);
    }

    public boolean isEmpty(ImmutableWALPointerBlockHydrator hydrator) throws Exception {
        return hydrateAsNeeded(hydrator).isEmpty(); // TODO optimize
    }

    public long size(ImmutableWALPointerBlockHydrator hydrator) throws Exception {
        return hydrateAsNeeded(hydrator).size();// TODO optimize
    }

    public boolean rangeScan(ImmutableWALPointerBlockHydrator hydrator, byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        return hydrateAsNeeded(hydrator).rangeScan(from, to, stream); // TODO optimize
    }

    public boolean rowScan(ImmutableWALPointerBlockHydrator hydrator, WALKeyPointerStream stream) throws Exception {
        return hydrateAsNeeded(hydrator).rowScan(stream); // TODO optimize
    }

}
