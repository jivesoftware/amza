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

public interface RowsStorage extends RowScanable {

    void load() throws Exception;

    // TODO Consider using a call back stream instead of returning RowsChanged
    RowsChanged update(RowScanable rowUpdates) throws Exception;

    RowIndexValue get(RowIndexKey key);

    boolean containsKey(RowIndexKey key);

    void takeRowUpdatesSince(final long transactionId, RowScan rowUpdates) throws Exception;

    void compactTombestone(long ifOlderThanNMillis) throws Exception;

    void clear() throws Exception;

}
