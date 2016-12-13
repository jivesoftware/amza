package com.jivesoftware.os.amzabot.deployable.bot;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface AmzaBotRandomOpConfig extends Config {

    @BooleanDefault(false)
    boolean getEnabled();
    void setEnabled(boolean value);

    @IntDefault(10)
    int getHesitationFactorMs();
    void setHesitationFactorMs(int value);

    @LongDefault(10_000L)
    long getWriteThreshold();
    void setWriteThreshold(long value);

    @IntDefault(100)
    int getValueSizeThreshold();
    void setValueSizeThreshold(int value);

    @StringDefault("fsync_async")
    String getDurability();
    void setDurability(String value);

    @StringDefault("leader_quorum")
    String getConsistency();
    void setConsistency(String value);

    @IntDefault(3)
    int getRingSize();
    void setRingSize(int value);

    @IntDefault(5_000)
    int getRetryWaitMs();
    void setRetryWaitMs(int value);

    @IntDefault(1_000)
    int getSnapshotFrequency();
    void setSnapshotFrequency(int value);

    @BooleanDefault(true)
    boolean getClientOrdering();
    void setClientOrdering(boolean value);

    @IntDefault(1_000)
    int getBatchFactor();
    void setBatchFactor(int value);

    @IntDefault(24 * 60 * 60 * 1_000)
    int getTombstoneTimestampAgeInMillis();
    void setTombstoneTimestampAgeInMillis(int value);

    @IntDefault(12 * 60 * 60 * 1_000)
    int getTombstoneTimestampIntervalMillis();
    void setTombstoneTimestampIntervalMillis(int value);

}
