package com.jivesoftware.os.amza.api.stream;

public interface TxKeyValueStream {

    TxResult stream(long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) throws Exception;

    enum TxResult {
        MORE(true, true),
        ACCEPT_AND_STOP(true, false),
        REJECT_AND_STOP(false, false);

        private final boolean accepted;
        private final boolean more;

        TxResult(boolean accepted, boolean more) {
            this.accepted = accepted;
            this.more = more;
        }

        public boolean isAccepted() {
            return accepted;
        }

        public boolean wantsMore() {
            return more;
        }
    }
}
