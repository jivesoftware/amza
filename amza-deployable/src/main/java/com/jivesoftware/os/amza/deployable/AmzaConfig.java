package com.jivesoftware.os.amza.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface AmzaConfig extends Config {

    @StringDefault("./var/data")
    String getWorkingDirs();

    @IntDefault(1000)
    int getTakeFromNeighborsIntervalInMillis();

    @LongDefault(60_000)
    long getCheckIfCompactionIsNeededIntervalInMillis();

    @IntDefault(8)
    int getNumberOfCompactorThreads();

    @IntDefault(8)
    int getNumberOfTakerThreads();

    @LongDefault(30_000L)
    long getDiscoveryIntervalMillis();

}
