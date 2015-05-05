package com.jivesoftware.os.amza.service.storage.delta;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
class OverConsumingEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    private final Iterator<Map.Entry<K, V>> backingIterator;
    private Map.Entry<K, V> next = null;
    private Map.Entry<K, V> peek = null;

    public OverConsumingEntryIterator(Iterator<Map.Entry<K, V>> backingIterator) {
        this.backingIterator = backingIterator;
    }

    private void findNext() {
        if (next != null) {
            return;
        }

        next = peek;
        peek = null;
        while (backingIterator.hasNext()) {
            peek = backingIterator.next();
            if (next == null || peek.getKey().equals(next.getKey())) {
                next = peek;
                peek = null;
            } else {
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        findNext();
        return next != null;
    }

    @Override
    public Map.Entry<K, V> next() {
        findNext();
        Map.Entry<K, V> got = next;
        next = null;
        return got;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Nope");
    }
}
