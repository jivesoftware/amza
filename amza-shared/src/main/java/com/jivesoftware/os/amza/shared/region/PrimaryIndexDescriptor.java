package com.jivesoftware.os.amza.shared.region;

import java.util.Map;

/**
 * @author jonathan.colt
 */
public class PrimaryIndexDescriptor {

    public String className;
    public long ttlInMillis = 0;
    public boolean forceCompactionOnStartup = false;
    public Map<String, String> properties;

    public PrimaryIndexDescriptor() {
    }

    public PrimaryIndexDescriptor(String className, long ttlInMillis, boolean forceCompactionOnStartup, Map<String, String> properties) {
        this.className = className;
        this.ttlInMillis = ttlInMillis;
        this.forceCompactionOnStartup = forceCompactionOnStartup;
        this.properties = properties;
    }

}
