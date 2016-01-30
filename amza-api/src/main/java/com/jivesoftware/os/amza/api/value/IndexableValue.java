package com.jivesoftware.os.amza.api.value;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class IndexableValue {

    public static class PathToKeyPart {

        public final List<byte[]> pathToValue;

        public PathToKeyPart(List<byte[]> pathToValue) {
            this.pathToValue = pathToValue;
        }

    }

    public static class IndexableKey {

        public final List<PathToKeyPart> keyParts;

        public IndexableKey(List<PathToKeyPart> keyParts) {
            this.keyParts = keyParts;
        }

    }

    public static class IndexPartition {

        public final String amzaClusterId; //??
        public final PartitionName partitionName;
        public final PartitionProperties partitionProperties;
        public final int ringSize;
        public final IndexableKey indexableKey;
        public final String indexableValue = "datastructure";

        public IndexPartition(String amzaClusterId, PartitionName partitionName, PartitionProperties partitionProperties, int ringSize,
            IndexableKey indexableKey) {
            this.amzaClusterId = amzaClusterId;
            this.partitionName = partitionName;
            this.partitionProperties = partitionProperties;
            this.ringSize = ringSize;
            this.indexableKey = indexableKey;
        }

    }

    public byte[] toIndexKey(TraversableValue traverse, IndexableKey indexableKey) {
        List<PathToKeyPart> parts = indexableKey.keyParts;
        byte[][] keyParts = new byte[parts.size()][];
        int i = 0;
        int size = 0;
        for (PathToKeyPart keyPart : parts) {
            for (byte[] traverseTo : keyPart.pathToValue) {
                traverse = traverse.traverse(traverseTo);
                if (traverse == null) {
                    throw new IllegalStateException("Failed to build key element is absent"); // TODO argue
                }
            }
            if (traverse instanceof Value) {
                keyParts[i] = ((Value) traverse).value;
                size += keyParts[i].length;
            } else {
                throw new IllegalStateException("Last element is not a value"); // TODO argue
            }
        }

        byte[] indexKey = new byte[size];
        int offset = 0;
        for (byte[] keyPart : keyParts) {
            System.arraycopy(indexKey, offset, keyPart, 0, keyPart.length);
            offset += keyPart.length;
        }
        return indexKey;

    }

    public static interface TraversableValue {

        TraversableValue traverse(byte[] value);
    }

    public static class MapOf implements TraversableValue {

        public final BAHash<TraversableValue> map = new BAHash<>(new BAHLinkedMapState<byte[], TraversableValue>(10, true, BAHLinkedMapState.NIL), BAHasher.SINGLETON,
            BAEqualer.SINGLETON);

        @Override
        public TraversableValue traverse(byte[] value) {
            return map.get(value, 0, value.length);
        }
    }


    public static class ListOf implements TraversableValue {

        public final List<TraversableValue> values = new ArrayList<>();

        @Override
        public TraversableValue traverse(byte[] value) {
           return values.get(UIO.bytesInt(value));
        }
    }

    public static class Value implements TraversableValue {

        public final byte[] value;

        public Value(byte[] value) {
            this.value = value;
        }

        @Override
        public TraversableValue traverse(byte[] value) {
            return this;
        }

    }
}
