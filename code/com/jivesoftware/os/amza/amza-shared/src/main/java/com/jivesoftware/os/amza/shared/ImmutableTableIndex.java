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

public class ImmutableTableIndex implements TableIndex {

    private final TableIndex tableIndex;

    public ImmutableTableIndex(TableIndex tableIndex) {
        this.tableIndex = tableIndex;
    }

    @Override
    public TimestampedValue put(TableIndexKey key, TimestampedValue value) {
        throw new UnsupportedOperationException("Cannot put into an immutable table index.");
    }

    @Override
    public TimestampedValue get(TableIndexKey key) {
        return tableIndex.get(key);
    }

    @Override
    public boolean containsKey(TableIndexKey key) {
        return tableIndex.containsKey(key);
    }

    @Override
    public TimestampedValue remove(TableIndexKey key) {
        throw new UnsupportedOperationException("Cannot remove from an immutable table index.");
    }

    @Override
    public <E extends Throwable> void entrySet(EntryStream<E> entryStream) {
        tableIndex.entrySet(entryStream);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Cannot clear an immutable table index.");
    }

    @Override
    public void flush() {
        tableIndex.flush();
    }

    @Override
    public boolean isEmpty() {
        return tableIndex.isEmpty();
    }

}
