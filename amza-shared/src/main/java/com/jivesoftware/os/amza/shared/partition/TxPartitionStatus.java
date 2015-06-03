package com.jivesoftware.os.amza.shared.partition;

import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public interface TxPartitionStatus {

    public static enum Status {

        EXPUNGE(new byte[]{2}), ONLINE(new byte[]{1}), KETCHUP(new byte[]{0});
        private final byte[] serializedForm;

        private Status(byte[] serializedForm) {
            this.serializedForm = serializedForm;
        }

        public byte[] getSerializedForm() {
            return serializedForm;
        }

        public static Status fromSerializedForm(byte[] bytes) {
            for (Status s : values()) {
                if (Arrays.equals(s.getSerializedForm(), bytes)) {
                    return s;
                }
            }
            return null;
        }
    }

    <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception;

}
