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
package com.jivesoftware.os.amza.shared.ring;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.SignedBytes;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RingMember implements Comparable<RingMember> {

    public byte[] toBytes() { // TODO convert to lex byte ordering?
        byte[] bytes = new byte[1 + memberAsBytes.length];
        bytes[0] = 0; // version;
        UIO.bytes(memberAsBytes, bytes, 1);
        return bytes;
    }

    public static RingMember fromBytes(byte[] bytes) throws IOException {
        if (bytes[0] == 0) {
            String member = new String(bytes, 1, bytes.length - 1, StandardCharsets.UTF_8);
            return new RingMember(member);
        }
        return null;
    }


    private final String member;
    private final byte[] memberAsBytes;

    @JsonCreator
    public RingMember(@JsonProperty("member") String member) {
        this.member = member;
        this.memberAsBytes = member.getBytes(StandardCharsets.UTF_8);
    }

    public String getMember() {
        return member;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Arrays.hashCode(this.memberAsBytes);
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
        RingMember other = (RingMember) obj;
        return Arrays.equals(this.memberAsBytes, other.memberAsBytes);
    }

    @Override
    public String toString() {
        return "RingMember{" + "member=" + member + '}';
    }

    @Override
    public int compareTo(RingMember o) {
        return SignedBytes.lexicographicalComparator().compare(memberAsBytes, o.memberAsBytes);
    }
}
