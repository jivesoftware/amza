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
package com.jivesoftware.os.amza.service;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AmzaPartitionWatcher implements RowChanges {

    private final boolean systemWatcher;
    private final RowChanges rowChanges;
    private final Map<PartitionName, List<RowChanges>> watchers = Maps.newConcurrentMap();

    public AmzaPartitionWatcher(boolean systemWatcher, RowChanges rowChanges) {
        this.systemWatcher = systemWatcher;
        this.rowChanges = rowChanges;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        rowChanges.changes(changes);
        if (!watchers.isEmpty()) {
            List<RowChanges> changeWatchers = watchers.get(changes.getVersionedPartitionName().getPartitionName());
            if (changeWatchers != null) {
                for (RowChanges watcher : changeWatchers) {
                    watcher.changes(changes);
                }
            }
        }
    }

    public void watch(PartitionName partitionName, RowChanges rowChanges) throws Exception {
        if ((systemWatcher && !partitionName.isSystemPartition()) || (!systemWatcher && partitionName.isSystemPartition())) {
            throw new IllegalArgumentException("This watch doesn't support this type of partition. Expect:" + systemWatcher + "  Is: " + partitionName
                .isSystemPartition());
        }
        watchers.computeIfAbsent(partitionName, (t) -> new ArrayList<>()).add(rowChanges);
    }

}
