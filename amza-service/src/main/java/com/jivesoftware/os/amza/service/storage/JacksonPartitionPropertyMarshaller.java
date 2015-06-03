package com.jivesoftware.os.amza.service.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;

/**
 *
 * @author jonathan.colt
 */
public class JacksonPartitionPropertyMarshaller implements PartitionPropertyMarshaller {

    private final ObjectMapper mapper;

    public JacksonPartitionPropertyMarshaller(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public PartitionProperties fromBytes(byte[] bytes) throws Exception {
        return mapper.readValue(bytes, PartitionProperties.class);
    }

    @Override
    public byte[] toBytes(PartitionProperties partitionProperties) throws Exception {
        return mapper.writeValueAsBytes(partitionProperties);
    }

}
