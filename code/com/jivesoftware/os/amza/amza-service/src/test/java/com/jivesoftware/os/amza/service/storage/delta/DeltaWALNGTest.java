package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryWALTx;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class DeltaWALNGTest {

    @Test
    public void testLoad() throws Exception {
        RegionName regionName = new RegionName(true, "test", "test");
        File tmp = Files.createTempDir();
        final RowMarshaller<byte[]> marshaller = new BinaryRowMarshaller();
        WALTx walTX = new BinaryWALTx(tmp, "test", new BinaryRowIOProvider(new IoStats()), marshaller, new NoOpWALIndexProvider());
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        DeltaWAL deltaWAL = new DeltaWAL(regionName, ids,
            marshaller,
            walTX);

        Map<WALKey, WALValue> apply1 = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            apply1.put(new WALKey((i + "k").getBytes()), new WALValue((i + "v").getBytes(), ids.nextId(), false));
        }
        DeltaWAL.DeltaWALApplied update1 = deltaWAL.update(regionName, apply1);
        for (Entry<WALKey, byte[]> e : update1.keyToRowPointer.entrySet()) {
            System.out.println("update1 k=" + new String(e.getKey().getKey()) + " fp=" + UIO.bytesLong(e.getValue()));
        }

        Map<WALKey, WALValue> apply2 = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            apply2.put(new WALKey((i + "k").getBytes()), new WALValue((i + "v").getBytes(), ids.nextId(), false));
        }
        DeltaWAL.DeltaWALApplied update2 = deltaWAL.update(regionName, apply1);
        for (Entry<WALKey, byte[]> e : update2.keyToRowPointer.entrySet()) {
            System.out.println("update2 k=" + new String(e.getKey().getKey()) + " fp=" + UIO.bytesLong(e.getValue()));
        }

        deltaWAL.load(new RowStream() {

            @Override
            public boolean row(long rowFP, long rowTxId, byte rowType, byte[] rawRow) throws Exception {
                RowMarshaller.WALRow row = marshaller.fromRow(rawRow);
                ByteBuffer bb = ByteBuffer.wrap(row.getKey().getKey());
                byte[] regionNameBytes = new byte[bb.getShort()];
                bb.get(regionNameBytes);
                byte[] keyBytes = new byte[bb.getInt()];
                bb.get(keyBytes);

                System.out.println("rfp=" + rowFP + " rid" + rowTxId + " rt=" + rowType
                    + " key=" + new String(keyBytes) + " value=" + new String(row.getValue().getValue())
                    + " ts=" + row.getValue().getTimestampId() + " tombstone=" + row.getValue().getTombstoned()
                );
                return true;
            }
        });

        for (Entry<WALKey, byte[]> e : update1.keyToRowPointer.entrySet()) {
            System.out.println("hydrate:" + new String(e.getKey().getKey()) + " @ fp=" + UIO.bytesLong(e.getValue()));
            WALValue hydrate = deltaWAL.hydrate(regionName, new WALPointer(UIO.bytesLong(e.getValue()), 0, false));
            System.out.println(new String(hydrate.getValue()));
            Assert.assertEquals(hydrate.getValue(), apply1.get(e.getKey()).getValue());
        }

        for (Entry<WALKey, byte[]> e : update2.keyToRowPointer.entrySet()) {
            System.out.println("hydrate:" + new String(e.getKey().getKey()) + " @ fp=" + UIO.bytesLong(e.getValue()));
            WALValue hydrate = deltaWAL.hydrate(regionName, new WALPointer(UIO.bytesLong(e.getValue()), 0, false));
            System.out.println(new String(hydrate.getValue()));
            Assert.assertEquals(hydrate.getValue(), apply2.get(e.getKey()).getValue());
        }

    }

}
