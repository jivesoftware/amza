package com.jivesoftware.os.amza.embed;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface AmzaConfig extends Config {

    @StringDefault("./var/data")
    String getWorkingDirs();

    @LongDefault(60_000L)
    long getTakeSlowThresholdInMillis();

    @IntDefault(8)
    int getNumberOfTakerThreads();

    @LongDefault(1_000L)
    long getAsyncFsyncIntervalMillis();

    @BooleanDefault(true)
    boolean getUseMemMap();

    @IntDefault(2)
    int getTombstoneCompactionFactor();

    @LongDefault(60_000)
    long getCheckIfCompactionIsNeededIntervalInMillis();

    @LongDefault(1000 * 60 * 60)
    long getRebalanceableEveryNMillis();

    @LongDefault(1024 * 1024 * 1024)
    long getRebalanceIfImbalanceGreaterThanNBytes();

    @LongDefault(60_000)
    long getInterruptBlockingReadsIfLingersForNMillis();

    @BooleanDefault(true)
    boolean getRackDistributionEnabled();

    @IntDefault(1_000_000)
    int getMaxUpdatesBeforeDeltaStripeCompaction();

    @StringDefault("")
    String getBlacklistRingMembers();
}
