package com.jivesoftware.os.amza.api.partition;

/**
 * @author jonathan.colt
 */
public interface TxPartitionStatus {

    enum Status {

        EXPUNGE((byte) 2),
        ONLINE((byte) 1),
        KETCHUP((byte) 0);

        private final byte serializedForm;

        Status(byte serializedForm) {
            this.serializedForm = serializedForm;
        }

        public byte getSerializedForm() {
            return serializedForm;
        }

        public static Status fromSerializedForm(byte serializedForm) {
            for (Status s : values()) {
                if (s.getSerializedForm() == serializedForm) {
                    return s;
                }
            }
            return null;
        }
    }

    <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception;

    VersionedStatus getLocalStatus(PartitionName partitionName) throws Exception;

}
