package com.jivesoftware.os.amza.transport.http.replication.endpoints;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaRestClient {

    StateMessageCause commit(PartitionName partitionName, Consistency consistency, boolean checkLeader, long partitionAwaitOnlineTimeoutMillis, IReadable read)
        throws Exception;

    RingTopology configPartition(PartitionName partitionName, PartitionProperties partitionProperties, int ringSize) throws Exception;

    void configPartition(RingTopology ring, IWriteable writeable) throws Exception;

    void ensurePartition(PartitionName partitionName, long waitForLeaderElection) throws Exception;

    void get(PartitionName partitionName, Consistency consistency, IReadable in, IWriteable out) throws Exception;

    RingLeader ring(PartitionName partitionName, long waitForLeaderElection) throws Exception;

    void ring(RingLeader ringLeader, IWriteable writeable) throws IOException;

    void scan(PartitionName partitionName, IReadable in, IWriteable out) throws Exception;

    StateMessageCause status(PartitionName partitionName, Consistency consistency, boolean checkLeader, long partitionAwaitOnlineTimeoutMillis);

    void takeFromTransactionId(PartitionName partitionName, IReadable in, IWriteable out) throws Exception;

    void takePrefixFromTransactionId(PartitionName partitionName, IReadable in, IWriteable out) throws Exception;

    public static class RingLeader {

        final RingTopology ringTopology;
        final RingMember leader;

        public RingLeader(RingTopology ringTopology, RingMember leader) {
            this.ringTopology = ringTopology;
            this.leader = leader;
        }

    }

    public static enum State {
        ok, failed_to_come_online, lacks_leader, not_the_leader, error
    }

    public static class StateMessageCause {

        public final PartitionName partitionName;
        public final Consistency consistency;
        public final boolean checkLeader;
        public final long partitionAwaitOnlineTimeoutMillis;
        public final State state;
        public final String message;
        public final Exception cause;

        public StateMessageCause(PartitionName partitionName,
            Consistency consistency,
            boolean checkLeader,
            long partitionAwaitOnlineTimeoutMillis,
            State state,
            String message,
            Exception cause) {
            this.partitionName = partitionName;
            this.consistency = consistency;
            this.checkLeader = checkLeader;
            this.partitionAwaitOnlineTimeoutMillis = partitionAwaitOnlineTimeoutMillis;
            this.state = state;
            this.message = message;
            this.cause = cause;
        }

        @Override
        public String toString() {
            return "StateMessageCause{"
                + "partitionName=" + partitionName
                + ", consistency=" + consistency
                + ", checkLeader=" + checkLeader
                + ", partitionAwaitOnlineTimeoutMillis=" + partitionAwaitOnlineTimeoutMillis
                + ", state=" + state
                + ", message=" + message
                + ", cause=" + cause
                + '}';
        }

    }
}
