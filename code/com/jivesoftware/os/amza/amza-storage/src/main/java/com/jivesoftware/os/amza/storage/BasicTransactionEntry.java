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
package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TimestampedValue;

public class BasicTransactionEntry implements TransactionEntry {

    private final long orderId;
    private final TableIndexKey key;
    private final TimestampedValue value;

    public BasicTransactionEntry(long orderId, TableIndexKey key, TimestampedValue value) {
        this.orderId = orderId;
        this.key = key;
        this.value = value;
    }

    @Override
    public long getOrderId() {
        return orderId;
    }

    @Override
    public TableIndexKey getKey() {
        return key;
    }

    @Override
    public TimestampedValue getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "BasicTransactionEntry{" + "orderId=" + orderId + ", key=" + key + ", value=" + value + '}';
    }
}
