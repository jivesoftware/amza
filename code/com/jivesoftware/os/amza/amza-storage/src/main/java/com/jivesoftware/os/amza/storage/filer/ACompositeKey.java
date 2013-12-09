/*
 * ACompositeKey.java.java
 *
 * Created on 03-12-2010 11:21:54 PM
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

/**
 *
 * @param <K>
 */
abstract public class ACompositeKey<K> implements Comparable<ACompositeKey<K>> {

    /**
     *
     */
    protected K[] keys;

    /**
     *
     * @param _keys
     */
    public ACompositeKey(K... _keys) {
        keys = _keys;
    }

    /**
     *
     * @param _index
     * @return
     */
    public K key(int _index) {
        return keys[_index];
    }

    /**
     *
     * @return
     */
    public K[] getKeys() {
        return keys;
    }

    /**
     *
     * @return
     */
    public long getCount() {
        return keys.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (keys == null) {
            return sb.toString();
        }
        for (K key : keys) {
            if (key != null) {
                sb.append("-> ").append(key.toString());
            } else {
                sb.append(" null");
            }
        }
        return sb.toString();
    }
}
