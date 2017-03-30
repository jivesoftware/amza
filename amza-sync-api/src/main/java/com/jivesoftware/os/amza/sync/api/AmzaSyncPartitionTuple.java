package com.jivesoftware.os.amza.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Created by jonathan.colt on 1/13/17.
 */
public class AmzaSyncPartitionTuple {
    public final PartitionName from;
    public final PartitionName to;

    @JsonCreator
    public AmzaSyncPartitionTuple(@JsonProperty("from") PartitionName from,
        @JsonProperty("to") PartitionName to) {

        Preconditions.checkNotNull(from);
        Preconditions.checkNotNull(to);

        this.from = from;
        this.to = to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AmzaSyncPartitionTuple)) {
            return false;
        }

        AmzaSyncPartitionTuple that = (AmzaSyncPartitionTuple) o;

        if (!from.equals(that.from)) {
            return false;
        }
        return to.equals(that.to);

    }

    @Override
    public int hashCode() {
        int result = from.hashCode();
        result = 31 * result + to.hashCode();
        return result;
    }

    public static byte[] toBytes(AmzaSyncPartitionTuple tuple) {
        return toKeyString(tuple).getBytes(StandardCharsets.UTF_8);
    }

    public static String toKeyString(AmzaSyncPartitionTuple tuple) {
        return tuple.from.toBase64() + " " + tuple.to.toBase64();
    }

    public static AmzaSyncPartitionTuple fromBytes(byte[] bytes, int offset, AmzaInterner interner) throws InterruptedException {
        return fromKeyString(new String(bytes, offset, bytes.length - offset, StandardCharsets.UTF_8), interner);
    }

    public static AmzaSyncPartitionTuple fromKeyString(String key, AmzaInterner interner) throws InterruptedException {
        Iterable<String> fromTo = Splitter.on(' ').split(key);
        Iterator<String> iterator = fromTo.iterator();
        PartitionName from = iterator.hasNext() ? interner.internPartitionNameBase64(iterator.next()) : null;
        PartitionName to = iterator.hasNext() ? interner.internPartitionNameBase64(iterator.next()) : null;
        return new AmzaSyncPartitionTuple(from, to);
    }
}
