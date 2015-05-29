package com.jivesoftware.os.amza.berkeleydb;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class BerkeleyDBWALIndexName {

    public static enum Prefix {

        active,
        compacting,
        compacted,
        backup;
    }

    private final Prefix prefix;
    private final String name;

    public BerkeleyDBWALIndexName(Prefix prefix, String name) {
        this.prefix = prefix;
        this.name = name;
    }

    public String getName() {
        return prefix.toString() + "-" + name;
    }

    public BerkeleyDBWALIndexName prefixName(Prefix prefix) {
        return new BerkeleyDBWALIndexName(prefix, name);
    }

    public List<BerkeleyDBWALIndexName> all() {
        List<BerkeleyDBWALIndexName> all = new ArrayList<>();
        for (Prefix v : Prefix.values()) {
            all.add(prefixName(v));
        }
        return all;
    }

    @Override
    public String toString() {
        return "BerkeleyDBWALIndexName{" + "prefix=" + prefix + ", name=" + name + '}';
    }
}
