package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 *
 * @author jonathan.colt
 */
final class DeltaPeekableElmoIterator implements Iterator<Map.Entry<WALKey, WALPointer>> {
    private final Iterator<Map.Entry<WALKey, WALPointer>> iterator;
    private final Iterator<Map.Entry<WALKey, WALPointer>> compactingIterator;
    private Map.Entry<WALKey, WALPointer> last;
    private Map.Entry<WALKey, WALPointer> iNext;
    private Map.Entry<WALKey, WALPointer> cNext;

    public DeltaPeekableElmoIterator(Iterator<Map.Entry<WALKey, WALPointer>> iterator, Iterator<Map.Entry<WALKey, WALPointer>> compactingIterator) {
        this.iterator = iterator;
        this.compactingIterator = compactingIterator;
    }

    public void eos() {
        last = null;
    }

    public Map.Entry<WALKey, WALPointer> last() {
        return last;
    }

    @Override
    public boolean hasNext() {
        return (iNext != null || cNext != null) || iterator.hasNext() || compactingIterator.hasNext();
    }

    @Override
    public Map.Entry<WALKey, WALPointer> next() {
        if (iNext == null && iterator.hasNext()) {
            iNext = iterator.next();
        }
        if (cNext == null && compactingIterator.hasNext()) {
            cNext = compactingIterator.next();
        }
        if (iNext != null && cNext != null) {
            int compare = iNext.getKey().compareTo(cNext.getKey());
            if (compare == 0) {
                if (iNext.getValue().getTimestampId() > cNext.getValue().getTimestampId()) {
                    last = iNext;
                } else {
                    last = cNext;
                }
                iNext = null;
                cNext = null;
            } else if (compare < 0) {
                last = iNext;
                iNext = null;
            } else {
                last = cNext;
                cNext = null;
            }
        } else if (iNext != null) {
            last = iNext;
            iNext = null;
        } else if (cNext != null) {
            last = cNext;
            cNext = null;
        } else {
            throw new NoSuchElementException();
        }
        return last;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported ever!");
    }

}
