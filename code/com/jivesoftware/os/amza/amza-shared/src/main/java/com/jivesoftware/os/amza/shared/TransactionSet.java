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

import java.util.NavigableMap;

public class TransactionSet<K, V> {

    private final long highestTransactionId;
    private final NavigableMap<K, TimestampedValue<V>> changes;

    public TransactionSet(long highestTransactionId, NavigableMap<K, TimestampedValue<V>> changes) {
        this.highestTransactionId = highestTransactionId;
        this.changes = changes;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }

    public NavigableMap<K, TimestampedValue<V>> getChanges() {
        return changes;
    }

    @Override
    public String toString() {
        return "TransactionSet{" + "highestTransactionId=" + highestTransactionId + ", changes=" + changes + '}';
    }
}
