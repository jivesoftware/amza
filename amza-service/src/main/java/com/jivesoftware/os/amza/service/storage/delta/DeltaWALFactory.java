package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.api.wal.NoOpWALIndex;
import com.jivesoftware.os.amza.api.wal.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALTx;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class DeltaWALFactory {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final File walDir;
    private final RowIOProvider<File> ioProvider;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;

    public DeltaWALFactory(OrderIdProvider idProvider,
        File walDir,
        RowIOProvider<File> ioProvider,
        PrimaryRowMarshaller primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller) {
        this.idProvider = idProvider;
        this.walDir = walDir;
        this.ioProvider = ioProvider;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
    }

    public DeltaWAL<NoOpWALIndex> create() throws Exception {
        return createOrOpen(idProvider.nextId());
    }

    private DeltaWAL<NoOpWALIndex> createOrOpen(long id) throws Exception {
        WALTx<NoOpWALIndex> deltaWALRowsTx = new BinaryWALTx<>(walDir, String.valueOf(id), ioProvider, primaryRowMarshaller, new NoOpWALIndexProvider());
        deltaWALRowsTx.validateAndRepair();
        return new DeltaWAL<>(id, idProvider, primaryRowMarshaller, highwaterRowMarshaller, deltaWALRowsTx);
    }

    public List<DeltaWAL> list() throws Exception {
        List<DeltaWAL> deltaWALs = new ArrayList<>();
        for (String filename : BinaryWALTx.listExisting(walDir, ioProvider)) {
            try {
                long id = Long.parseLong(filename);
                deltaWALs.add(createOrOpen(id));
            } catch (Exception x) {
                LOG.warn("Encountered " + filename + " which doesn't conform to a WAL file naming conventions.");
            }
        }
        Collections.sort(deltaWALs);
        return deltaWALs;
    }
}
