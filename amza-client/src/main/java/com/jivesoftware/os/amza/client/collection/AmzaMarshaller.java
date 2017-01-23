package com.jivesoftware.os.amza.client.collection;

/**
 * Created by jonathan.colt on 1/16/17.
 */
public interface AmzaMarshaller<V> {

    V fromBytes(byte[] bytes) throws Exception;

    byte[] toBytes(V value) throws Exception;

}
