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
package com.jivesoftware.os.amza.api.ring;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.SignedBytes;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.aquarium.Member;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RingMember implements Comparable<RingMember> {

    public byte[] toBytes() { // TODO convert to lex byte ordering?
        byte[] bytes = new byte[1 + memberAsBytes.length];
        toBytes(bytes, 0);
        return bytes;
    }

    public int toBytes(byte[] bytes, int offset) {
        bytes[offset] = 0; // version;
        offset++;
        UIO.writeBytes(memberAsBytes, bytes, offset);
        return sizeInBytes();
    }

    public int sizeInBytes() {
        return 1 + memberAsBytes.length;
    }

    public static RingMember fromBytes(byte[] bytes, int offset, int length, BAInterner interner) throws InterruptedException {
        if (bytes != null && bytes[offset] == 0) {
            byte[] interned = interner.intern(bytes, offset + 1, length - 1);
            String member = new String(interned, StandardCharsets.UTF_8);

            return new RingMember(member);
        }
        return null;
    }

    public String toBase64() {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static RingMember fromBase64(String base64, BAInterner interner) throws InterruptedException {
        byte[] bytes = BaseEncoding.base64Url().decode(base64);
        return fromBytes(bytes, 0, bytes.length, interner);
    }

    private final byte[] memberAsBytes;

    private transient String member;
    private transient int hash = 0;

    @JsonCreator
    public RingMember(@JsonProperty("member") String member) {
        this.member = member;
        this.memberAsBytes = member.getBytes(StandardCharsets.UTF_8);
    }

    private RingMember(byte[] memberAsBytes) {
        this.memberAsBytes = memberAsBytes;
    }

    public byte[] leakBytes() {
        return memberAsBytes;
    }

    public String getMember() {
        if (member == null) {
            member = new String(memberAsBytes, StandardCharsets.UTF_8);
        }
        return member;
    }

    public Member asAquariumMember() {
        return new Member(memberAsBytes);
    }

    public static RingMember fromAquariumMember(Member member) {
        return new RingMember(member.getMember());
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int hashCode = 7;
            hashCode = 73 * hashCode + Arrays.hashCode(this.memberAsBytes);
            hash = hashCode;
        }
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
        return "RingMember{" + "member=" + getMember() + '}';
    }

    @Override
    public int compareTo(RingMember o) {
        return SignedBytes.lexicographicalComparator().compare(memberAsBytes, o.memberAsBytes);
    }
}
