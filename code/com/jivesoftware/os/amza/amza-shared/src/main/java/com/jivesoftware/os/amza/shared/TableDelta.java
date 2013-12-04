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

import com.google.common.collect.Multimap;
import java.util.NavigableMap;

public class TableDelta {

    private final NavigableMap<TableIndexKey, BinaryTimestampedValue> applyMap;
    private final NavigableMap<TableIndexKey, BinaryTimestampedValue> removeMap;
    private final Multimap<TableIndexKey, BinaryTimestampedValue> clobberedMap;

    public TableDelta(NavigableMap<TableIndexKey, BinaryTimestampedValue> applyMap,
            NavigableMap<TableIndexKey, BinaryTimestampedValue> removeMap,
            Multimap<TableIndexKey, BinaryTimestampedValue> clobberedMap) {
        this.applyMap = applyMap;
        this.removeMap = removeMap;
        this.clobberedMap = clobberedMap;
    }

    public NavigableMap<TableIndexKey, BinaryTimestampedValue> getApply() {
        return applyMap;
    }

    public NavigableMap<TableIndexKey, BinaryTimestampedValue> getRemove() {
        return removeMap;
    }

    public Multimap<TableIndexKey, BinaryTimestampedValue> getClobbered() {
        return clobberedMap;
    }
}