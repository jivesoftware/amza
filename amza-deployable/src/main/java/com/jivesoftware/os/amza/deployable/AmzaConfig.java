package com.jivesoftware.os.amza.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface AmzaConfig extends Config {

    @BooleanDefault(true)
    boolean getAutoDiscoveryEnabled();

    @StringDefault("amza")
    String getClusterName();

    @StringDefault("225.4.5.6")
    String getDiscoveryMulticastGroup();

    @IntDefault(1223)
    int getDiscoveryMulticastPort();

    @StringDefault("./var/data")
    String getWorkingDirs();

    @IntDefault(1000)
    int getResendReplicasIntervalInMillis();

    @IntDefault(1000)
    int getApplyReplicasIntervalInMillis();

    @IntDefault(1000)
    int getTakeFromNeighborsIntervalInMillis();

    @LongDefault(60_000)
    long getCheckIfCompactionIsNeededIntervalInMillis();

    @LongDefault(30 * 24 * 60 * 60 * 1000L)
    long getCompactTombstoneIfOlderThanNMillis();

    @IntDefault(8)
    int getNumberOfResendThreads();

    @IntDefault(8)
    int getNumberOfApplierThreads();

    @IntDefault(8)
    int getNumberOfCompactorThreads();

    @IntDefault(8)
    int getNumberOfTakerThreads();

    @IntDefault(24)
    int getNumberOfReplicatorThreads();

}
