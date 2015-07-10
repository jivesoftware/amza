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
package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.scan.RowStream;

public interface WALReader {

    /**
     * Scans from the given fp offset (must be the start of a row) to the end of the WAL.
     *
     * @param offsetFp the fp offset
     * @param allowRepairs whether to allow repairs/truncation if WAL corruption is detected (otherwise an exception is thrown)
     * @param rowStream the callback stream
     * @return true if the scan reaches the end of the WAL, otherwise false
     * @throws Exception if an error occurred
     */
    boolean scan(long offsetFp, boolean allowRepairs, RowStream rowStream) throws Exception;

    boolean reverseScan(RowStream rowStream) throws Exception;

    byte[] read(long position) throws Exception;
}
