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
    int getPartitionSize();
    void setPartitionSize(int value);

}
