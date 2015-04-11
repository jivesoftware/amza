package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class DeltaWALFactory {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final File walDir;
    private final RowIOProvider ioProvider;
    private final RowMarshaller<byte[]> rowMarshaller;

    public DeltaWALFactory(OrderIdProvider idProvider, File walDir, RowIOProvider ioProvider, RowMarshaller<byte[]> rowMarshaller) {
        this.idProvider = idProvider;
        this.walDir = walDir;
        this.ioProvider = ioProvider;
        this.rowMarshaller = rowMarshaller;
    }

    public DeltaWAL create() throws Exception {
        return createOrOpen(idProvider.nextId());
    }

    private DeltaWAL createOrOpen(long id) throws Exception {
        WALTx deltaWALRowsTx = new BinaryWALTx(walDir, String.valueOf(id), ioProvider, rowMarshaller, new NoOpWALIndexProvider());
        LOG.info("Created:" + walDir + "/" + id);
        return new DeltaWAL(id, idProvider, rowMarshaller, deltaWALRowsTx);
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
