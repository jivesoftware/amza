package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.region.RegionProperties;

/**
 *
 * @author jonathan.colt
 */
public interface RegionPropertyMarshaller {

    RegionProperties fromBytes(byte[] bytes) throws Exception;

    byte[] toBytes(RegionProperties regionProperties) throws Exception;
}
