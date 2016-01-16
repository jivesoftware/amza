package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public enum RowType {

    // when advancing a version, preserve old versions as legacy
    end_of_merge((byte) -2, false, false),
    system((byte) -1, true, false),
    highwater((byte) 0, false, false),
    primary((byte) 1, false, true),
    snappy_primary((byte) 2, false, true);

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
    private final boolean isPrimary;

    RowType(byte serialized, boolean discardDuringCompactions, boolean isPrimary) {
        this.serialized = serialized;
        this.discardDuringCompactions = discardDuringCompactions;
        this.isPrimary = isPrimary;
    }

    public boolean isDiscardedDuringCompactions() {
        return discardDuringCompactions;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    static public RowType fromByte(byte b) {
        return MAPPED[(int) b - Byte.MIN_VALUE];
    }

    public byte toByte() {
        return serialized;
    }
}
