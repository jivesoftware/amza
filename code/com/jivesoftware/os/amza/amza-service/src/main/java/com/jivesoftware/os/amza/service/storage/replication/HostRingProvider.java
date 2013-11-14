package com.jivesoftware.os.amza.service.storage.replication;

public interface HostRingProvider {

    HostRing getHostRing(String ringName) throws Exception;
}
