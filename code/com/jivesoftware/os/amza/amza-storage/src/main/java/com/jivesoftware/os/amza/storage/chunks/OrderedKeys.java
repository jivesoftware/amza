/*
 * OrderedKeys.java.java
 *
 * Created on 03-13-2010 07:39:47 AM
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
package com.jivesoftware.os.amza.storage.chunks;

// Important
// new OrderedKeys(new Object[]{a,b}).equals(new OrderedKeys(new Object[]{b,a})) == false
// new OrderedKeys(new Object[]{a,b}).hashCode() == new OrderedKeys(new Object[]{b,a}).hashCode() == false
/**
 *
 * @param <K>
 */
public class OrderedKeys<K> extends ACompositeKey<K> implements Comparable<ACompositeKey<K>> {

    /**
     *
     * @param _keys
     */
    public OrderedKeys(K... _keys) {
        super(_keys);
    }

    @Override
    final public int hashCode() {
        int hashCode = 0;
        if (keys == null) {
            return hashCode;
        }
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                hashCode += keys[i].hashCode();
            }
            hashCode *= hashCode; // Make hashCode ordered
        }
        return hashCode;
    }

    @Override
    final public boolean equals(Object instance) {
        if (instance == this) {
            return true;
        }
        if (!(instance instanceof OrderedKeys)) {
            return false;
        }
        OrderedKeys i = (OrderedKeys) instance;
        if (keys == null) {
            return i.keys == null;
        }
        if (i.keys == null) {
            return false;
        }
        if (keys.length != i.keys.length) {
            return false;
        }
        for (int a = 0; a < keys.length; a++) {
            if (keys[a] == null && i.keys[a] != null) {
                return false;
            }
            if (keys[a] != null && i.keys[a] == null) {
                return false;
            }
            if (keys[a] == i.keys[a]) {
                continue;
            }
            if (!keys[a].equals(i.keys[a])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(ACompositeKey<K> o) {
        if (!(o instanceof OrderedKeys)) {
            return -1;
        }
        if (equals(o)) {
            return 0;
        }
        return 1;
    }
}