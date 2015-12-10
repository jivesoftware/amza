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
    public PartitionProperties fromBytes(byte[] bytes) {
        try {
            return mapper.readValue(bytes, PartitionProperties.class);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] toBytes(PartitionProperties partitionProperties) {
        try {
            return mapper.writeValueAsBytes(partitionProperties);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
