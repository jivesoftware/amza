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
package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class TakeRowUpdatesRequestPayload implements MessagePayload {

    private RegionName mapName;
    private long highestTransactionId;

    public TakeRowUpdatesRequestPayload() {
    }

    public TakeRowUpdatesRequestPayload(RegionName mapName, long highestTransactionId) {
        this.mapName = mapName;
        this.highestTransactionId = highestTransactionId;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(mapName);
        output.writeLong(highestTransactionId);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.mapName = (RegionName) input.readObject();
        this.highestTransactionId = input.readLong();
    }

    public RegionName getMapName() {
        return mapName;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }
}
