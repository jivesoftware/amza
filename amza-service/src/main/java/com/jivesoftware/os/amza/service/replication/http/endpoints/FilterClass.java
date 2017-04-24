package com.jivesoftware.os.amza.service.replication.http.endpoints;

/**
 *
 */
public class FilterClass {

    public final byte[] md5;
    public final FilterClassLoader classLoader;

    public FilterClass(byte[] md5, FilterClassLoader classLoader) {
        this.md5 = md5;
        this.classLoader = classLoader;
    }
}
