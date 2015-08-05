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

import java.util.Arrays;

public class WALPrefix {

    public final byte[] prefix;

    private transient int hashCode = 0;

    public WALPrefix(byte[] prefix) {
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return "WALPrefix{" +
            "prefix=" + Arrays.toString(prefix) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WALPrefix walPrefix = (WALPrefix) o;

        if (hashCode != walPrefix.hashCode) {
            return false;
        }
        return Arrays.equals(prefix, walPrefix.prefix);

    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int result = prefix != null ? Arrays.hashCode(prefix) : 0;
            result = 31 * result + hashCode;
            hashCode = result;
        }
        return hashCode;
    }
}
