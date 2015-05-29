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

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import java.util.concurrent.ConcurrentHashMap;

public class AmzaRegionWatcher implements RowChanges {

    private final RowChanges rowChanges;
    private final ConcurrentHashMap<RegionName, RowChanges> watchers = new ConcurrentHashMap<>();

    public AmzaRegionWatcher(RowChanges rowChanges) {
        this.rowChanges = rowChanges;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        rowChanges.changes(changes);
        RowChanges watcher = watchers.get(changes.getVersionedRegionName());
        if (watcher != null) {
            watcher.changes(changes);
        }
    }

    public void watch(RegionName regionName, RowChanges rowChanges) throws Exception {
        watchers.put(regionName, rowChanges);
    }

    public RowChanges unwatch(RegionName regionName) throws Exception {
        return watchers.remove(regionName);
    }
}
