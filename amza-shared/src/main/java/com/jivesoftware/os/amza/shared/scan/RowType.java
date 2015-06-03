package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public enum RowType {

    // when advancing a version, preserve old versions as legacy
// when advancing a version, preserve old versions as legacy
    system((byte) -1, true),
    highwater((byte) 0, false),
    primary((byte) 1, false);

    public static final long COMPACTION_HINTS_KEY = 0;
    public static final long COMMIT_KEY = 1;
    public static final long LEAP_KEY = 2;

    private static final RowType[] MAPPED = new RowType[256];

    static {
        for (RowType rowType : values()) {
            MAPPED[(int) rowType.serialized - Byte.MIN_VALUE] = rowType;
        }
    }

    private final byte serialized;
    private final boolean discardDuringCompactions;

    RowType(byte serialized, boolean discardDuringCompactions) {
        this.serialized = serialized;
        this.discardDuringCompactions = discardDuringCompactions;
    }

    public boolean isDiscardedDuringCompactions() {
        return discardDuringCompactions;
    }

    static public RowType fromByte(byte b) {
        return MAPPED[(int) b - Byte.MIN_VALUE];
    }

    public byte toByte() {
        return serialized;
    }
}
