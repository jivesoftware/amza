/*
 * Copyright 2015 Jive Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.api.partition;

/**
 *
 * @author jonathan.colt
 */
public enum PartitionState {
    EXPUNGE((byte) 2), ONLINE((byte) 1), KETCHUP((byte) 0);
    private final byte serializedForm;

    PartitionState(byte serializedForm) {
        this.serializedForm = serializedForm;
    }

    public byte getSerializedForm() {
        return serializedForm;
    }

    public static PartitionState fromSerializedForm(byte serializedForm) {
        for (PartitionState s : PartitionState.values()) {
            if (s.getSerializedForm() == serializedForm) {
                return s;
            }
        }
        return null;
    }

}
