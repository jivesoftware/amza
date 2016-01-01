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

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AmzaPartitionWatcher implements RowChanges {

    private final RowChanges rowChanges;
    private final ConcurrentHashMap<PartitionName, List<RowChanges>> watchers = new ConcurrentHashMap<>();

    public AmzaPartitionWatcher(RowChanges rowChanges) {
        this.rowChanges = rowChanges;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        rowChanges.changes(changes);
        List<RowChanges> changeWatchers = watchers.get(changes.getVersionedPartitionName().getPartitionName());
        if (changeWatchers != null) {
            for (RowChanges watcher : changeWatchers) {
                watcher.changes(changes);
            }
        }
    }

    public void watch(PartitionName partitionName, RowChanges rowChanges) throws Exception {
        watchers.computeIfAbsent(partitionName, (PartitionName t) -> new ArrayList<>()).add(rowChanges);
    }

    public RowChanges unwatch(PartitionName partitionName) throws Exception {
        throw new UnsupportedOperationException("Need to figure out watcher removal");
    }
}
