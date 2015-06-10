package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaPartitionAPI {

    TakeQuorum commit(Commitable<WALValue> updates,
        int desiredQuorum,
        long timeoutInMillis) throws Exception;

    void get(Iterable<byte[]> keys, Scan<TimestampedValue> valuesStream) throws Exception;

    /**

     @param from nullable (inclusive)
     @param to nullable (exclusive)
     @param stream
     @throws Exception
     */
    void scan(byte[] from, byte[] to, Scan<TimestampedValue> stream) throws Exception;

    TakeResult takeFromTransactionId(long transactionId, Highwaters highwaters, Scan<TimestampedValue> scan) throws Exception;

    class TakeQuorum {

        private final RingMember commitedTo;
        private final long txId;
        private final Collection<RingHost> takeOrderHosts;

        public TakeQuorum(RingMember commitedTo, long txId, Collection<RingHost> takeOrderHosts) {
            this.commitedTo = commitedTo;
            this.txId = txId;
            this.takeOrderHosts = takeOrderHosts;
        }

        public RingMember getCommitedTo() {
            return commitedTo;
        }

        public long getTxId() {
            return txId;
        }

        public Collection<RingHost> getTakeOrderHosts() {
            return takeOrderHosts;
        }

    }

    class TimestampedValue {

        private final long timestampId;
        private final byte[] value;

        public TimestampedValue(long timestampId, byte[] value) {
            this.timestampId = timestampId;
            this.value = value;
        }

        public long getTimestampId() {
            return timestampId;
        }

        public byte[] getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "TimestampedValue{"
                + "timestampId=" + timestampId
                + ", value=" + Arrays.toString(value)
                + '}';
        }
    }

}
