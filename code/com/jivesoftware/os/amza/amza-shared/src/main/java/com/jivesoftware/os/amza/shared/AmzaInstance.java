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

    <K, V> void changes(TableName<K, V> tableName, TableDelta<K, V> changes) throws Exception;

    <K, V> void takeTableChanges(TableName<K, V> tableName, long transationId, TransactionSetStream<K, V> transactionSetStream) throws Exception;

    List<TableName> getTableNames();

    <K, V> void destroyTable(TableName<K, V> tableName) throws Exception;

    void addRingHost(String ringName, RingHost ringHost) throws Exception;

    void removeRingHost(String ringName, RingHost ringHost) throws Exception;

    List<RingHost> getRing(String ringName) throws Exception;
}
