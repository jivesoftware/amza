package com.jivesoftware.os.amza.berkeleydb;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class BerkeleyDBWALIndexName {

    public enum Type {

        active,
        compacting,
        compacted,
        backup;
    }

    private final Type type;
    private final String name;

    public BerkeleyDBWALIndexName(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getPrimaryName() {
        return "primary-" + type.toString() + "-" + name;
    }

    public String getPrefixName() {
        return "prefix-" + type.toString() + "-" + name;
    }

    public BerkeleyDBWALIndexName typeName(Type type) {
        return new BerkeleyDBWALIndexName(type, name);
    }

    public List<BerkeleyDBWALIndexName> all() {
        List<BerkeleyDBWALIndexName> all = new ArrayList<>();
        for (Type v : Type.values()) {
            all.add(typeName(v));
        }
        return all;
    }

    @Override
    public String toString() {
        return "BerkeleyDBWALIndexName{" + "type=" + type + ", name=" + name + '}';
    }
}
