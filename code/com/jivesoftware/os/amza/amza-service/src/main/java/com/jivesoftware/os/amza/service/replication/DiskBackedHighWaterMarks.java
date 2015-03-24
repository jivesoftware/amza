package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;

public class DiskBackedHighWaterMarks implements HighwaterMarks {

    @Override
    public void clearRing(RingHost ringHost) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void set(RingHost ringHost, RegionName regionName, long highWatermark) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear(RingHost ringHost, RegionName regionName) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Long get(RingHost ringHost, RegionName regionName) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
