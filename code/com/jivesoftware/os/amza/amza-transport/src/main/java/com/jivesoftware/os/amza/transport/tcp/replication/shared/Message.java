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
package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.Serializable;
import java.util.Objects;

/**
 *
 */
public class Message implements Serializable {

    private final long interactionId;
    private final int opCode;
    private final boolean lastInSequence;
    private final Serializable payload;

    public Message(long interactionId, int opCode, boolean lastInSequence, Serializable payload) {
        this.interactionId = interactionId;
        this.opCode = opCode;
        this.lastInSequence = lastInSequence;
        this.payload = payload;
    }

    public Message(long interactionId, int opCode, boolean lastInSequence) {
        this(interactionId, opCode, lastInSequence, null);
    }

    public long getInteractionId() {
        return interactionId;
    }

    public int getOpCode() {
        return opCode;
    }

    public boolean isLastInSequence() {
        return lastInSequence;
    }

    public <P extends Serializable> P getPayload() {
        return (P) payload;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (int) (this.interactionId ^ (this.interactionId >>> 32));
        hash = 29 * hash + this.opCode;
        hash = 29 * hash + (this.lastInSequence ? 1 : 0);
        hash = 29 * hash + Objects.hashCode(this.payload);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Message other = (Message) obj;
        if (this.interactionId != other.interactionId) {
            return false;
        }
        if (this.opCode != other.opCode) {
            return false;
        }
        if (this.lastInSequence != other.lastInSequence) {
            return false;
        }
        if (!Objects.equals(this.payload, other.payload)) {
            return false;
        }
        return true;
    }
}
