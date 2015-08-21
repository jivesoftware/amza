package com.jivesoftware.os.amza.api;

/**
 *
 * @author jonathan.colt
 */
public enum Consistency {

    /**
     Writes are routed to an elected node. No synchronous replication is performed.
     Reads are routed to an elected node. 
     */
    leader((byte) 1, true),
    /**
     Writes are routed to an elected node. Each write will be synchronously replication to at least one other node.
     Reads are routed to an elected node. If elected node is offline then a read request will re-routed to all other nodes and merged at the client.
     */
    leader_plus_one((byte) 2, true),
    /**
     Writes are routed to an elected node. Each write will be synchronously replication to an additional quorum of nodes based on ring size at write time.
     Reads are routed to an elected node. If elected node is offline then a read request will re-routed to a quorum of other nodes and merged at the client.
     */
    leader_quorum((byte) 3, true),
    /**
     Writes are synchronously replication to a quorum of nodes based on ring size at write time.
     Reads are routed to a quorum of nodes and merged at the client.
     */
    quorum((byte) 4, false),
    /**
     Writes are synchronously written to all nodes based on ring size at write time.
     Reads are routed to a random node.
     */
    write_all_read_one((byte) 5, false),
    /**
     Writes are to a random node based on ring size at write time.
     Reads are routed to all nodes and merged at the client.
     */
    write_one_read_all((byte) 6, false),
    /**
     Writes are to a random node based on ring size at write time.
     Reads are to a random node based on ring size at write time.
     */
    none((byte) 7, false);

    private final byte serializedByte;
    private final boolean requiresLeader;

    private Consistency(byte serializedByte, boolean requiresLeader) {
        this.serializedByte = serializedByte;
        this.requiresLeader = requiresLeader;
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

    public int quorum(int numberOfNeighbors) {
        if (this == leader_plus_one) {
            return Math.min(numberOfNeighbors, 1);
        } else if (this == leader_quorum || this == quorum) {
            return numberOfNeighbors / 2;
        } else if (this == write_all_read_one) {
            return numberOfNeighbors;
        }
        return 0;
    }
}
