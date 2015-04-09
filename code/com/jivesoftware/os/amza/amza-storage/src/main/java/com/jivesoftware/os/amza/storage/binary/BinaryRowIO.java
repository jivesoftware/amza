package com.jivesoftware.os.amza.storage.binary;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.amza.shared.filer.IFiler;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowIO implements RowIO, WALReader, WALWriter {

    private final File file;
    private final IFiler filer;
    private final BinaryRowReader rowReader;
    private final BinaryRowWriter rowWriter;

    public BinaryRowIO(File file, IFiler filer, BinaryRowReader rowReader, BinaryRowWriter rowWriter) {
        this.file = file;
        this.filer = filer;
        this.rowReader = rowReader;
        this.rowWriter = rowWriter;
    }

    @Override
    public void scan(long offset, RowStream rowStream) throws Exception {
        rowReader.scan(offset, rowStream);
    }

    @Override
    public void reverseScan(RowStream rowStream) throws Exception {
        rowReader.reverseScan(rowStream);
    }

    @Override
    public byte[] read(long pointer) throws Exception {
        return rowReader.read(pointer);
    }

    @Override
    public List<byte[]> write(List<Long> rowTxIds, List<Byte> rowTypes, List<byte[]> rows) throws Exception {
        return rowWriter.write(rowTxIds, rowTypes, rows);
    }

    @Override
    public long getEndOfLastRow() throws Exception {
        return rowWriter.getEndOfLastRow();
    }

    @Override
    public void move(File destinationDir) throws Exception {
        File destinationFile = new File(destinationDir, file.getName());
        Files.move(file, destinationFile);
    }

    @Override
    public long sizeInBytes() throws Exception {
        return filer.length();
    }

    @Override
    public void flush() throws Exception {
        filer.flush();
    }

    @Override
    public void close() throws IOException {
        filer.close();
    }

    @Override
    public void delete() throws Exception {
        FileUtils.deleteQuietly(file);
    }

}
