package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALTx;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class DeltaWALFactory {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final File walDir;
    private final RowIOProvider ioProvider;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final long corruptionParanoiaFactor;

    public DeltaWALFactory(OrderIdProvider idProvider,
        File walDir,
        RowIOProvider ioProvider,
        PrimaryRowMarshaller primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        long corruptionParanoiaFactor) {
        this.idProvider = idProvider;
        this.walDir = walDir;
        this.ioProvider = ioProvider;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
    }

    public DeltaWAL create(long prevId) throws Exception {
        return createOrOpen(idProvider.nextId(), prevId);
    }

    private DeltaWAL createOrOpen(long id, long prevId) throws Exception {
        WALTx deltaWALRowsTx = new BinaryWALTx(
            String.valueOf(prevId) + "_" + String.valueOf(id),
            ioProvider,
            primaryRowMarshaller,
            Integer.MAX_VALUE,
            64);
        MutableLong rows = new MutableLong();
        deltaWALRowsTx.open(walDir,
            io -> {
                io.validate(true, false,
                    (rowFP, rowTxId, rowType, row) -> {
                        rows.increment();
                        return (rows.longValue() < corruptionParanoiaFactor) ? -1 : rowFP;
                    },
                    (rowFP, rowTxId, rowType, row) -> -1,
                    null);
                return null;
            });
        return new DeltaWAL(id, prevId, idProvider, primaryRowMarshaller, highwaterRowMarshaller, deltaWALRowsTx);
    }

    public List<DeltaWAL> list() throws Exception {
        List<DeltaWAL> deltaWALs = new ArrayList<>();
        for (String filename : BinaryWALTx.listExisting(walDir, ioProvider)) {
            try {
                String[] parts = filename.split("_");
                long prevId = Long.parseLong(parts[0]);
                long id = Long.parseLong(parts[1]);
                deltaWALs.add(createOrOpen(id, prevId));
            } catch (Exception x) {
                LOG.warn("Encountered {} which doesn't conform to a WAL file naming conventions.", filename);
            }
        }
        Collections.sort(deltaWALs);
        return deltaWALs;
    }

    void destroy(DeltaWAL wal) throws Exception {
        wal.destroy(walDir);
    }
}
