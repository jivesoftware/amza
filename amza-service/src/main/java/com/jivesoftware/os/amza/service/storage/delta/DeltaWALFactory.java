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

    public DeltaWAL create() throws Exception {
        return createOrOpen(idProvider.nextId());
    }

    private DeltaWAL createOrOpen(long id) throws Exception {
        WALTx deltaWALRowsTx = new BinaryWALTx(walDir, String.valueOf(id), ioProvider, primaryRowMarshaller);
        MutableLong rows = new MutableLong();
        deltaWALRowsTx.open(io -> {
            io.validate(false,
                (rowFP, rowTxId, rowType, row) -> {
                    rows.increment();
                    return (rows.longValue() < corruptionParanoiaFactor) ? -1 : rowFP;
                },
                (rowFP, rowTxId, rowType, row) -> -1,
                null);
            return null;
        });
        return new DeltaWAL(id, idProvider, primaryRowMarshaller, highwaterRowMarshaller, deltaWALRowsTx);
    }

    public List<DeltaWAL> list() throws Exception {
        List<DeltaWAL> deltaWALs = new ArrayList<>();
        for (String filename : BinaryWALTx.listExisting(walDir, ioProvider)) {
            try {
                long id = Long.parseLong(filename);
                deltaWALs.add(createOrOpen(id));
            } catch (Exception x) {
                LOG.warn("Encountered {} which doesn't conform to a WAL file naming conventions.", filename);
            }
        }
        Collections.sort(deltaWALs);
        return deltaWALs;
    }
}
