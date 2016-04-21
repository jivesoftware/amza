package com.jivesoftware.os.amza.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface AmzaConfig extends Config {

    @StringDefault("./var/data")
    String getWorkingDirs();

    @IntDefault(1000)
    int getTakeFromNeighborsIntervalInMillis();

    @IntDefault(8)
    int getNumberOfCompactorThreads();

    @IntDefault(8)
    int getNumberOfTakerThreads();

    @LongDefault(30_000L)
    long getDiscoveryIntervalMillis();

    @LongDefault(1_000L)
    long getAsyncFsyncIntervalMillis();

    @BooleanDefault(false)
    boolean getUseMemMap();

    @IntDefault(2)
    int getTombstoneCompactionFactor();

    @LongDefault(60_000)
    long getCheckIfCompactionIsNeededIntervalInMillis();

    @LongDefault(1000 * 60 * 60)
    long getRebalanceableEveryNMillis();

    @LongDefault(1024 * 1024 * 1024)
    long getRebalanceIfImbalanceGreaterThanNBytes();

}
