package com.jivesoftware.os.amza.api.value;

/**
 *
 * @author jonathan.colt
 */
public interface BAH<V> {

    void clear();

    V get(byte[] key, int keyOffset, int keyLength);

    V get(long hashCode, byte[] key, int keyOffset, int keyLength);

    void put(byte[] key, int keyOffset, int keyLength, V value);

    @SuppressWarnings(value = "unchecked")
    void put(long hashCode, byte[] key, int keyOffset, int keyLength, V value);

    void remove(byte[] key, int keyOffset, int keyLength);

    @SuppressWarnings(value = "unchecked")
    void remove(long hashCode, byte[] key, int keyOffset, int keyLength);

    long size();

    boolean stream(KeyValueStream<byte[], V> stream) throws Exception;

    void dump();

}
