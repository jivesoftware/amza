package com.jivesoftware.os.amza.sync.api;

/**
 * Created by jonathan.colt on 1/16/17.
 */
public interface AmzaConfigMarshaller<V> {

    V fromBytes(byte[] bytes) throws Exception;

    byte[] toBytes(V value) throws Exception;

}
