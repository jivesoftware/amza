package com.jivesoftware.os.amza.api.partition;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public enum Consistency {

    /**
     * Writes are routed to an elected node. No synchronous replication is performed.
     * Reads are routed to an elected node.
     */
    leader((byte) 1, true,
        numberOfNeighbors -> 0,
        numberOfNeighbors -> {
            throw new UnsupportedOperationException("Leader consistency has no repair quorum, please take from the demoted leader");
        }),
    /**
     * Writes are routed to an elected node. Each write will be synchronously replicated to at least one other node.
     * Reads are routed to an elected node. If elected node is offline then a read request will re-routed to all other nodes and merged at the client.
     */
    leader_plus_one((byte) 2, true,
        numberOfNeighbors -> {
            if (numberOfNeighbors == 0) {
                throw new IllegalArgumentException("Leader plus one consistency requires at least one neighbor");
            }
            return 1;
        },
        numberOfNeighbors -> numberOfNeighbors - 1),
    /**
     * Writes are routed to an elected node. Each write will be synchronously replicated to an additional quorum of nodes based on ring size at write time.
     * Reads are routed to an elected node. If elected node is offline then a read request will re-routed to a quorum of other nodes and merged at the client.
     */
    leader_quorum((byte) 3, true,
        numberOfNeighbors -> (numberOfNeighbors + 1) / 2,
        numberOfNeighbors -> numberOfNeighbors / 2),
    /**
     * Writes are routed to an elected node. Each write will be synchronously replicated to all nodes based on ring size at write time.
     * Reads are routed to an elected node. If elected node is offline then a read request will re-routed to a random node.
     */
    leader_all((byte) 4, true,
        numberOfNeighbors -> numberOfNeighbors,
        numberOfNeighbors -> 0),
    /**
     * Writes are synchronously replicated to a quorum of nodes based on ring size at write time.
     * Reads are routed to a quorum of nodes and merged at the client.
     */
    quorum((byte) 11, false,
        numberOfNeighbors -> (numberOfNeighbors + 1) / 2,
        numberOfNeighbors -> numberOfNeighbors / 2),
    /**
     * Writes are synchronously written to all nodes based on ring size at write time.
     * Reads are routed to a random node.
     */
    write_all_read_one((byte) 21, false,
        numberOfNeighbors -> numberOfNeighbors,
        numberOfNeighbors -> 0),
    /**
     * Writes are to a random node based on ring size at write time.
     * Reads are routed to all nodes and merged at the client.
     */
    write_one_read_all((byte) 22, false,
        numberOfNeighbors -> 0,
        numberOfNeighbors -> numberOfNeighbors),
    /**
     * Writes are to a random node based on ring size at write time.
     * Reads are to a random node based on ring size at write time.
     */
    none((byte) 127, false,
        numberOfNeighbors -> 0,
        numberOfNeighbors -> 0);

    private static final Set<Consistency>[] WRITES_SUPPORT;
    private static final Set<Consistency>[] READS_SUPPORT;

    static {
        @SuppressWarnings("unchecked")
        Set<Consistency>[] writesSupport = new Set[Consistency.values().length];
        writesSupport[leader.ordinal()] = ImmutableSet.of(leader, leader_plus_one, leader_quorum, leader_all, write_all_read_one);
        writesSupport[leader_plus_one.ordinal()] = ImmutableSet.of(leader_plus_one, leader_quorum, leader_all, write_all_read_one);
        writesSupport[leader_quorum.ordinal()] = ImmutableSet.of(leader_quorum, leader_all, write_all_read_one);
        writesSupport[leader_all.ordinal()] = ImmutableSet.of(leader_all, write_all_read_one);
        writesSupport[quorum.ordinal()] = ImmutableSet.of(leader_quorum, leader_all, quorum, write_all_read_one);
        writesSupport[write_all_read_one.ordinal()] = ImmutableSet.of(leader_all, write_all_read_one);
        writesSupport[write_one_read_all.ordinal()] = ImmutableSet.copyOf(Consistency.values());
        writesSupport[none.ordinal()] = ImmutableSet.copyOf(Consistency.values());
        WRITES_SUPPORT = writesSupport;

        @SuppressWarnings("unchecked")
        Set<Consistency>[] readsSupport = new Set[Consistency.values().length];
        readsSupport[leader.ordinal()] = ImmutableSet.of(leader, write_one_read_all, none);
        readsSupport[leader_plus_one.ordinal()] = ImmutableSet.of(leader, leader_plus_one, write_one_read_all, none);
        readsSupport[leader_quorum.ordinal()] = ImmutableSet.of(leader, leader_quorum, quorum, write_one_read_all, none);
        readsSupport[leader_all.ordinal()] = ImmutableSet.copyOf(Consistency.values());
        readsSupport[quorum.ordinal()] = ImmutableSet.of(quorum, write_one_read_all, none);
        readsSupport[write_all_read_one.ordinal()] = ImmutableSet.copyOf(Consistency.values());
        readsSupport[write_one_read_all.ordinal()] = ImmutableSet.of(write_one_read_all, none);
        readsSupport[none.ordinal()] = ImmutableSet.copyOf(Consistency.values());
        READS_SUPPORT = readsSupport;
    }

    private final byte serializedByte;
    private final boolean requiresLeader;
    private final NumberOfNeighborsQuorum writeQuorum;
    private final NumberOfNeighborsQuorum repairQuorum;

    Consistency(byte serializedByte, boolean requiresLeader, NumberOfNeighborsQuorum writeQuorum, NumberOfNeighborsQuorum repairQuorum) {
        this.serializedByte = serializedByte;
        this.requiresLeader = requiresLeader;
        this.writeQuorum = writeQuorum;
        this.repairQuorum = repairQuorum;
    }

    public boolean requiresLeader() {
        return requiresLeader;
    }

    public byte toBytes() {
        return serializedByte;
    }

    static Consistency fromBytes(byte[] b) {
        for (Consistency v : values()) {
            if (v.serializedByte == b[0]) {
                return v;
            }
        }
        return null;
    }

    public boolean supportsWrites(Consistency consistency) {
        return WRITES_SUPPORT[ordinal()].contains(consistency);
    }

    public boolean supportsReads(Consistency consistency) {
        return READS_SUPPORT[ordinal()].contains(consistency);
    }

    interface NumberOfNeighborsQuorum {
        int quorum(int numberOfNeighbors);
    }

    public int quorum(int numberOfNeighbors) {
        return writeQuorum.quorum(numberOfNeighbors);
    }

    public int repairQuorum(int numberOfNeighbors) {
        return repairQuorum.quorum(numberOfNeighbors);
    }
}
