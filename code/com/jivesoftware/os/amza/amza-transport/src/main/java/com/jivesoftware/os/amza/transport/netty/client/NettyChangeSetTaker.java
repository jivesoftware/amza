package com.jivesoftware.os.amza.transport.netty.client;

import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSetStream;


public class NettyChangeSetTaker implements ChangeSetTaker {

    @Override
    public <K, V> void take(RingHost ringHost,
            TableName<K, V> partitionName,
            long transationId,
            TransactionSetStream transactionSetStream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}