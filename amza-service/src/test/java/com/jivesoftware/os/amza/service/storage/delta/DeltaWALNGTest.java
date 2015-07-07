package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.wal.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaWALNGTest {

    @Test
    public void testLoad() throws Exception {
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(true, "test", "test"), 1);
        File tmp = Files.createTempDir();
        final PrimaryRowMarshaller<byte[]> primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
        final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();

        WALTx walTX = new BinaryWALTx(tmp, "test", new BinaryRowIOProvider(new IoStats(), 1, false), primaryRowMarshaller, new NoOpWALIndexProvider(), -1);
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        DeltaWAL deltaWAL = new DeltaWAL(0, ids, primaryRowMarshaller, highwaterRowMarshaller, walTX);

        Map<WALKey, WALValue> apply1 = Maps.newLinkedHashMap();
        for (int i = 0; i < 10; i++) {
            apply1.put(new WALKey((i + "k").getBytes()), new WALValue((i + "v").getBytes(), ids.nextId(), false));
        }
        DeltaWAL.DeltaWALApplied update1 = deltaWAL.update(versionedPartitionName, apply1, null);
        for (DeltaWAL.KeyValueHighwater kvh : update1.keyValueHighwaters) {
            System.out.println("update1 k=" + new String(kvh.key.getKey()) + " v=" + kvh.value);
        }

        Map<WALKey, WALValue> apply2 = Maps.newLinkedHashMap();
        for (int i = 0; i < 10; i++) {
            apply2.put(new WALKey((i + "k").getBytes()), new WALValue((i + "v").getBytes(), ids.nextId(), false));
        }
        DeltaWAL.DeltaWALApplied update2 = deltaWAL.update(versionedPartitionName, apply1, null);
        for (DeltaWAL.KeyValueHighwater kvh : update2.keyValueHighwaters) {
            System.out.println("update2 k=" + new String(kvh.key.getKey()) + " v=" + kvh.value);
        }

        deltaWAL.load((long rowFP, long rowTxId, RowType rowType, byte[] rawRow) -> {
            if (rowType == RowType.primary) {
                WALRow row = primaryRowMarshaller.fromRow(rawRow);
                ByteBuffer bb = ByteBuffer.wrap(row.key.getKey());
                byte[] partitionNameBytes = new byte[bb.getShort()];
                bb.get(partitionNameBytes);
                byte[] keyBytes = new byte[bb.getInt()];
                bb.get(keyBytes);

                System.out.println("rfp=" + rowFP + " rid" + rowTxId + " rt=" + rowType
                    + " key=" + new String(keyBytes) + " value=" + new String(row.value.getValue())
                    + " ts=" + row.value.getTimestampId() + " tombstone=" + row.value.getTombstoned());
            }
            return true;
        });

        for (int i = 0; i < update1.fps.length; i++) {
            DeltaWAL.KeyValueHighwater kvh = update1.keyValueHighwaters[i];
            System.out.println("hydrate k=" + new String(kvh.key.getKey()) + " v=" + kvh.value);
            WALValue hydrate = deltaWAL.hydrate(update1.fps[i]).value;
            System.out.println(new String(hydrate.getValue()));
            Assert.assertEquals(hydrate.getValue(), apply1.get(kvh.key).getValue());
        }

        for (int i = 0; i < update2.fps.length; i++) {
            DeltaWAL.KeyValueHighwater kvh = update2.keyValueHighwaters[i];
            System.out.println("hydrate k=" + new String(kvh.key.getKey()) + " v=" + kvh.value);
            WALValue hydrate = deltaWAL.hydrate(update2.fps[i]).value;
            System.out.println(new String(hydrate.getValue()));
            Assert.assertEquals(hydrate.getValue(), apply2.get(kvh.key).getValue());
        }
    }
}
