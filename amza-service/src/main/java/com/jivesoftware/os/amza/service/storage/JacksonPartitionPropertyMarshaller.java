package com.jivesoftware.os.amza.service.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import java.io.IOException;

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
    public PartitionProperties fromBytes(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, PartitionProperties.class);

    }

    @Override
    public byte[] toBytes(PartitionProperties partitionProperties) throws IOException {
        return mapper.writeValueAsBytes(partitionProperties);
    }

}
