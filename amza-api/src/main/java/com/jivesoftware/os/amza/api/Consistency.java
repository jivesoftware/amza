package com.jivesoftware.os.amza.api;

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
     * Writes are routed to an elected node. Each write will be synchronously replication to at least one other node.
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
     * Writes are routed to an elected node. Each write will be synchronously replication to an additional quorum of nodes based on ring size at write time.
     * Reads are routed to an elected node. If elected node is offline then a read request will re-routed to a quorum of other nodes and merged at the client.
     */
    leader_quorum((byte) 3, true,
        numberOfNeighbors -> (numberOfNeighbors + 1) / 2,
        numberOfNeighbors -> numberOfNeighbors / 2),
    /**
     * Writes are synchronously replication to a quorum of nodes based on ring size at write time.
     * Reads are routed to a quorum of nodes and merged at the client.
     */
    quorum((byte) 4, false,
        numberOfNeighbors -> (numberOfNeighbors + 1) / 2,
        numberOfNeighbors -> numberOfNeighbors / 2),
    /**
     * Writes are synchronously written to all nodes based on ring size at write time.
     * Reads are routed to a random node.
     */
    write_all_read_one((byte) 5, false,
        numberOfNeighbors -> numberOfNeighbors,
        numberOfNeighbors -> 0),
    /**
     * Writes are to a random node based on ring size at write time.
     * Reads are routed to all nodes and merged at the client.
     */
    write_one_read_all((byte) 6, false,
        numberOfNeighbors -> 0,
        numberOfNeighbors -> numberOfNeighbors),
    /**
     * Writes are to a random node based on ring size at write time.
     * Reads are to a random node based on ring size at write time.
     */
    none((byte) 7, false,
        numberOfNeighbors -> 0,
        numberOfNeighbors -> 0);

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
