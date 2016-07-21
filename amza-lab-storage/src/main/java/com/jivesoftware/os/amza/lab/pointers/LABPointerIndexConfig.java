package com.jivesoftware.os.amza.lab.pointers;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface LABPointerIndexConfig extends Config {

    @IntDefault(4)
    int getMinMergeDebt();

    @IntDefault(16)
    int getMaxMergeDebt();

    @BooleanDefault(true)
    boolean getUseMemMap();

    @IntDefault(4096)
    int getEntriesBetweenLeaps();

    @LongDefault(-1)
    long getSplitWhenKeysTotalExceedsNBytes();

    @LongDefault(-1)
    long getSplitWhenValuesTotalExceedsNBytes();

    @LongDefault(10 * 1024 * 1024)
    long getSplitWhenValuesAndKeysTotalExceedsNBytes();

    @IntDefault(24)
    int getConcurrency();

    @LongDefault(1_000_000)
    long getLeapCacheMaxCapacity();

    void setLeapCacheMaxCapacity(long leapCacheMaxCapacity);

    @LongDefault(60_000)
    long getLeapCacheCleanupIntervalInMillis();

    @LongDefault(1024 * 1024 * 1024)
    long getGlobalMaxHeapPressureInBytes();

    @LongDefault(1024 * 1024)
    long getMaxHeapPressureInBytes();
}
