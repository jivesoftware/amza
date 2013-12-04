/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.shared;

public interface TableIndex {

    static public interface EntryStream<E extends Throwable> {

        boolean stream(TableIndexKey key, TimestampedValue value) throws E;
    }

    TimestampedValue put(TableIndexKey key, TimestampedValue value);

    TimestampedValue get(TableIndexKey key);

    boolean containsKey(TableIndexKey key);

    TimestampedValue remove(TableIndexKey key);

    <E extends Throwable> void entrySet(EntryStream<E> entryStream);

    boolean isEmpty();

    void clear();

    /**
     * Force persistence of all changes
     */
    void flush();
}
