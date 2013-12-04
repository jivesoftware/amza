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

import java.util.List;

public interface AmzaInstance {

    void changes(TableName tableName, TableDelta changes) throws Exception;

    void takeTableChanges(TableName tableName, long transationId, TransactionSetStream transactionSetStream) throws Exception;

    List<TableName> getTableNames();

    void destroyTable(TableName tableName) throws Exception;

    void addRingHost(String ringName, RingHost ringHost) throws Exception;

    void removeRingHost(String ringName, RingHost ringHost) throws Exception;

    List<RingHost> getRing(String ringName) throws Exception;
}
