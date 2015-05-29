package com.jivesoftware.os.amza.shared;

import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public interface TxRegionStatus {

    public static enum Status {

        DISPOSE(new byte[]{2}), ONLINE(new byte[]{1}), KETCHUP(new byte[]{0});
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

    <R> R tx(RegionName regionName, RegionTx<R> tx) throws Exception;

}
