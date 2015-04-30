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

public interface WALWriter {

    public static final byte SYSTEM_VERSION_1 = -1;
    public static final byte VERSION_1 = 1;

    public static final long COMPACTION_HINTS_KEY = 0;
    public static final long COMMIT_KEY = 1;
    public static final long LEAP_KEY = 2;

    long[] write(List<Long> txId, List<Byte> rowType, List<byte[]> rows) throws Exception;

    long writeSystem(byte[] row) throws Exception;

    long getEndOfLastRow() throws Exception;
}
