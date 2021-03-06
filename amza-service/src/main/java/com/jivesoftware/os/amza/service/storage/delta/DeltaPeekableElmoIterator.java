package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.WALPointer;
import com.jivesoftware.os.amza.api.wal.WALValue;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * TODO replace with stream!
 *
 * @author jonathan.colt
 */
class DeltaPeekableElmoIterator implements Iterator<Map.Entry<byte[], WALValue>> {

    private final Iterator<Map.Entry<byte[], WALPointer>> iterator;
    private final Iterator<Map.Entry<byte[], WALPointer>> compactingIterator;
    private final WALRowHydrator hydrator;
    private final WALRowHydrator compactingHydrator;
    private final boolean hydrateValues;
    private Map.Entry<byte[], WALValue> last;
    private Map.Entry<byte[], WALPointer> iNext;
    private Map.Entry<byte[], WALPointer> cNext;

    public DeltaPeekableElmoIterator(Iterator<Map.Entry<byte[], WALPointer>> iterator,
        Iterator<Map.Entry<byte[], WALPointer>> compactingIterator,
        WALRowHydrator hydrator,
        WALRowHydrator compactingHydrator,
        boolean hydrateValues) {
        this.iterator = new OverConsumingEntryIterator<>(iterator);
        this.compactingIterator = new OverConsumingEntryIterator<>(compactingIterator);
        this.hydrator = hydrator;
        this.compactingHydrator = compactingHydrator;
        this.hydrateValues = hydrateValues;
    }

    public void eos() {
        last = null;
    }

    public Map.Entry<byte[], WALValue> last() {
        return last;
    }

    @Override
    public boolean hasNext() {
        return (iNext != null || cNext != null) || iterator.hasNext() || compactingIterator.hasNext();
    }

    @Override
    public Map.Entry<byte[], WALValue> next() {
        if (iNext == null && iterator.hasNext()) {
            iNext = iterator.next();
        }
        if (cNext == null && compactingIterator.hasNext()) {
            cNext = compactingIterator.next();
        }
        if (iNext != null && cNext != null) {
            int compare = KeyUtil.compare(iNext.getKey(), cNext.getKey());
            if (compare == 0) {
                if (iNext.getValue().getTimestampId() > cNext.getValue().getTimestampId()) {
                    last = hydrate(iNext, hydrator);
                } else {
                    last = hydrate(cNext, compactingHydrator);
                }
                iNext = null;
                cNext = null;
            } else if (compare < 0) {
                last = hydrate(iNext, hydrator);
                iNext = null;
            } else {
                last = hydrate(cNext, compactingHydrator);
                cNext = null;
            }
        } else if (iNext != null) {
            last = hydrate(iNext, hydrator);
            iNext = null;
        } else if (cNext != null) {
            last = hydrate(cNext, compactingHydrator);
            cNext = null;
        } else {
            throw new NoSuchElementException();
        }
        return last;
    }

    // TODO fix this Ugly Abomination
    private Map.Entry<byte[], WALValue> hydrate(Map.Entry<byte[], WALPointer> entry, WALRowHydrator valueHydrator) {
        try {
            WALPointer pointer = entry.getValue();
            WALValue hydrated = null;
            if (hydrateValues) {
                if (pointer.getHasValue()) {
                    hydrated = new WALValue(RowType.primary, pointer.getValue(), pointer.getTimestampId(), pointer.getTombstoned(), pointer.getVersion());
                } else {
                    hydrated = valueHydrator.hydrate(entry.getValue().getFp());
                }
            } else {
                hydrated = new WALValue(RowType.primary, null, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getVersion());
            }
            return new AbstractMap.SimpleEntry<>(entry.getKey(), hydrated);
        } catch (Exception e) {
            throw new RuntimeException("Failed to hydrate while iterating delta", e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported ever!");
    }

    public void close() {
        if (compactingHydrator != null) {
            compactingHydrator.closeHydrator();
        }
    }
}
