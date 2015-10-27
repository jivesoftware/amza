package com.jivesoftware.os.amza.lsm;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class LSMPointerIndexWALIndexName {

    public enum Type {

        active,
        compacting,
        compacted,
        backup;
    }

    private final Type type;
    private final String name;

    public LSMPointerIndexWALIndexName(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getPrimaryName() {
        return "primary-" + type.toString() + "-" + name;
    }

    public String getPrefixName() {
        return "prefix-" + type.toString() + "-" + name;
    }

    public LSMPointerIndexWALIndexName typeName(Type type) {
        return new LSMPointerIndexWALIndexName(type, name);
    }

    public List<LSMPointerIndexWALIndexName> all() {
        List<LSMPointerIndexWALIndexName> all = new ArrayList<>();
        for (Type v : Type.values()) {
            all.add(typeName(v));
        }
        return all;
    }

    @Override
    public String toString() {
        return "LSMPointerIndexWALIndexName{" + "type=" + type + ", name=" + name + '}';
    }
}
