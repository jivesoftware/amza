package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.api.scan.RowType;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.wal.NoOpWALIndex;
import com.jivesoftware.os.amza.shared.wal.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaWALNGTest {

    @Test
    public void testLoad() throws Exception {
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(true, "test".getBytes(), "test".getBytes()), 1);
        File tmp = Files.createTempDir();
        final PrimaryRowMarshaller<byte[]> primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
        final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();

        WALTx<NoOpWALIndex> walTX = new BinaryWALTx<>(tmp, "test", new BinaryRowIOProvider(new IoStats(), 1, false), primaryRowMarshaller,
            new NoOpWALIndexProvider());
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        DeltaWAL<NoOpWALIndex> deltaWAL = new DeltaWAL<>(0, ids, primaryRowMarshaller, highwaterRowMarshaller, walTX);

        Map<WALKey, WALValue> apply1 = Maps.newLinkedHashMap();
        for (int i = 0; i < 10; i++) {
            byte[] bytes = (i + "k").getBytes();
            apply1.put(new WALKey(bytes, bytes), new WALValue((i + "v").getBytes(), ids.nextId(), false));
        }
        DeltaWAL.DeltaWALApplied update1 = deltaWAL.update(versionedPartitionName, apply1, null);
        for (DeltaWAL.KeyValueHighwater kvh : update1.keyValueHighwaters) {
            System.out.println("update1 k=" + new String(kvh.key) + " v=" + new WALValue(kvh.value, kvh.valueTimestamp, kvh.valueTombstone));
        }

        Map<WALKey, WALValue> apply2 = Maps.newLinkedHashMap();
        for (int i = 0; i < 10; i++) {
            byte[] bytes = (i + "k").getBytes();
            apply2.put(new WALKey(bytes, bytes), new WALValue((i + "v").getBytes(), ids.nextId(), false));
        }
        DeltaWAL.DeltaWALApplied update2 = deltaWAL.update(versionedPartitionName, apply2, null);
        for (DeltaWAL.KeyValueHighwater kvh : update2.keyValueHighwaters) {
            System.out.println("update2 k=" + new String(kvh.key) + " v=" + new WALValue(kvh.value, kvh.valueTimestamp, kvh.valueTombstone));
        }

        deltaWAL.load((long rowFP, long rowTxId, RowType rowType, byte[] rawRow) -> {
            if (rowType == RowType.primary) {
                primaryRowMarshaller.fromRows(
                    (PrimaryRowMarshaller.FpRows) fpRowStream -> fpRowStream.stream(rowFP, rawRow),
                    (fp, prefix, key, value, valueTimestamp, valueTombstoned) -> {
                        System.out.println("rfp=" + rowFP + " rid" + rowTxId + " rt=" + rowType
                            + " key=" + new String(key) + " value=" + new String(value)
                            + " ts=" + valueTimestamp + " tombstone=" + valueTombstoned);
                        return true;
                    });
            }
            return true;
        });

        for (int i = 0; i < update1.fps.length; i++) {
            DeltaWAL.KeyValueHighwater kvh = update1.keyValueHighwaters[i];
            System.out.println(update1.fps[i] + " hydrate k=" + new String(kvh.key) + " v=" + new WALValue(kvh.value, kvh.valueTimestamp, kvh.valueTombstone));
            long fp1 = update1.fps[i];
            deltaWAL.hydrate(fpStream -> fpStream.stream(fp1),
                (fp, prefix, key, value, valueTimestamp, valueTombstone) -> {
                    System.out.println(fp + " hydrated:" + new WALValue(value, valueTimestamp, valueTombstone));
                    Assert.assertEquals(new WALValue(value, valueTimestamp, valueTombstone), apply1.get(new WALKey(kvh.prefix, kvh.key)));
                    return true;
                });
        }

        for (int i = 0; i < update2.fps.length; i++) {
            DeltaWAL.KeyValueHighwater kvh = update2.keyValueHighwaters[i];
            System.out.println(update2.fps[i] + " hydrate k=" + new String(kvh.key) + " v=" + new WALValue(kvh.value, kvh.valueTimestamp, kvh.valueTombstone));
            long fp2 = update2.fps[i];
            deltaWAL.hydrate(fpStream -> fpStream.stream(fp2),
                (fp, prefix, key, value, valueTimestamp, valueTombstone) -> {
                    System.out.println(fp + " hydrated:" + new WALValue(value, valueTimestamp, valueTombstone));
                    Assert.assertEquals(new WALValue(value, valueTimestamp, valueTombstone), apply2.get(new WALKey(kvh.prefix, kvh.key)));
                    return true;
                });

        }
    }
}
