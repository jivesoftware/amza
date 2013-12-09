/*
 * SoftIndexRef.java.java
 *
 * Created on 03-12-2010 11:24:38 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.storage.filer;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

/**
 *
 * @author Administrator
 * @param <V>
 * @param <K>
 * @param <P>
 */
public class SoftIndexRef<V, K, P> extends SoftReference<V> {

    K _key;
    P _payload;

    SoftIndexRef(V value, K key, P payload, ReferenceQueue queue) {
        super(value, queue);
        _key = key;
        _payload = payload;
    }

    /**
     *
     * @return
     */
    public P getPayload() {
        return _payload;
    }

    /**
     *
     * @return
     */
    public K getKey() {
        return _key;
    }

    /**
     *
     */
    public void dispose() {
        _key = null;
        _payload = null;
    }

    @Override
    public int hashCode() {
        return _key.hashCode();
    }

    @Override
    public boolean equals(Object instance) {
        if (_key == null) {
            return false;
        }
        if (instance == this) {
            return true;
        }
        if (instance instanceof SoftIndexRef) {
            return _key.equals(((SoftIndexRef) instance)._key);
        }
        return _key.equals(instance);
    }
}