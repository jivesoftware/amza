package com.jivesoftware.os.amza.lab.pointers;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface LABPointerIndexConfig extends Config {

    @IntDefault(4)
    int getMinMergeDebt();

    @IntDefault(16)
    int getMaxMergeDebt();

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

    @StringDefault("amza")
    String getHeapPressureName();

    @LongDefault(1024 * 1024 * 1024 * 2L)
    long getGlobalBlockOnHeapPressureInBytes();

    @LongDefault(1024 * 1024 * 1024)
    long getGlobalMaxHeapPressureInBytes();

    @LongDefault(1024 * 1024)
    long getMaxHeapPressureInBytes();

    @LongDefault(1024 * 1024 * 10)
    long getMaxWALSizeInBytes();

    @LongDefault(100_000)
    long getMaxEntriesPerWAL();

    @LongDefault(1024 * 1024 * 1024)
    long getMaxEntrySizeInBytes();

    @LongDefault(1024 * 1024 * 1024)
    long getMaxWALOnOpenHeapPressureOverride();

    @DoubleDefault(0d)
    double getHashIndexLoadFactor();

    @StringDefault("cuckoo")
    String getHashIndexType();

    @BooleanDefault(true)
    boolean getHashIndexEnabled();

}
