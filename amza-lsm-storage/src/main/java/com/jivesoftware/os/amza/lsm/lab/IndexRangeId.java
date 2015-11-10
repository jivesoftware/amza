package com.jivesoftware.os.amza.lsm.lab;

import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public class IndexRangeId implements Comparable<IndexRangeId> {

    final long start;
    final long end;

    public IndexRangeId(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public boolean intersects(IndexRangeId range) {
        return (start <= range.start && end >= range.start) || (start <= range.end && end >= range.end);
    }

    @Override
    public int compareTo(IndexRangeId o) {
        int c = Long.compare(start, o.start);
        if (c == 0) {
            c = Long.compare(o.end, end); // reversed
        }
        return c;
    }

    public File toFile(File parent) {
        return new File(parent, start + "-" + end);
    }

    public IndexRangeId join(IndexRangeId id) {
        return new IndexRangeId(Math.min(start, id.start), Math.max(end, id.end));
    }

}
