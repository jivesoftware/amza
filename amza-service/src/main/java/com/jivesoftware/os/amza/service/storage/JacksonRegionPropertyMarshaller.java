package com.jivesoftware.os.amza.service.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.shared.region.RegionProperties;

/**
 *
 * @author jonathan.colt
 */
public class JacksonRegionPropertyMarshaller implements RegionPropertyMarshaller {

    private final ObjectMapper mapper;

    public JacksonRegionPropertyMarshaller(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public RegionProperties fromBytes(byte[] bytes) throws Exception {
        return mapper.readValue(bytes, RegionProperties.class);
    }

    @Override
    public byte[] toBytes(RegionProperties regionProperties) throws Exception {
        return mapper.writeValueAsBytes(regionProperties);
    }

}
