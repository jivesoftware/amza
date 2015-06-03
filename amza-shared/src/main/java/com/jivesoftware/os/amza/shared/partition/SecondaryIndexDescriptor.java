package com.jivesoftware.os.amza.shared.partition;

import java.util.Map;

/**
 *
 * @author jonathan.colt
 */
public class SecondaryIndexDescriptor {

    public String className;
    public Map<String, String> properties;

    public SecondaryIndexDescriptor() {
    }

    public SecondaryIndexDescriptor(String className, Map<String, String> properties) {
        this.className = className;
        this.properties = properties;
    }

}
