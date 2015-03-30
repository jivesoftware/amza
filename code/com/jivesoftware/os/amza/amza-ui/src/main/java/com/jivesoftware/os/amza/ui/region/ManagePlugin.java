package com.jivesoftware.os.amza.ui.region;

/**
 *
 */
public class ManagePlugin {

    public final String glyphicon;
    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final Region<?> region;

    public ManagePlugin(String glyphicon, String name, String path, Class<?> endpointsClass, Region<?> region) {
        this.glyphicon = glyphicon;
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
    }
}
