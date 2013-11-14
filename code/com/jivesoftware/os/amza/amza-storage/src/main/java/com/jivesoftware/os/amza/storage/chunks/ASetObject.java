/*
 * ASetObject.java.java
 *
 * Created on 03-12-2010 11:22:02 PM
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

abstract public class ASetObject<E> {

    /**
     *
     * @return
     */
    abstract public E hashObject();

    @Override
    public String toString() {
        Object object = hashObject();
        if (object == null) {
            return super.toString();
        }
        if (object == this) {
            return super.toString();
        }
        return object.toString();
    }

    @Override
    public int hashCode() {
        E object = hashObject();
        if (object == this) {
            return super.hashCode();
        }
        if (object == null) {
            return 0;
        }
        return object.hashCode();
    }

    @Override
    public boolean equals(Object b) {
        E a = hashObject();
        if (a == this) {
            return super.equals(b);
        }
        if (b == this) {
            return true;
        }
        try {
            if (b instanceof ASetObject) {
                Object bObject = ((ASetObject) b).hashObject();
                if (a == bObject) {
                    return true;
                }
                return a.equals(bObject);
            }
            return a.equals(b);
        } catch (Exception x) {
            return false;
        }
    }
}