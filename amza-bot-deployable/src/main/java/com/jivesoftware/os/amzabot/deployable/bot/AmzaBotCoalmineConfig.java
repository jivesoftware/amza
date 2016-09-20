package com.jivesoftware.os.amzabot.deployable.bot;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface AmzaBotCoalmineConfig extends Config {

    @BooleanDefault(false)
    boolean getEnabled();
    void setEnabled(boolean value);

    @LongDefault(300_000L)
    long getFrequencyMs();
    void setFrequencyMs(long value);

    @LongDefault(10_000L)
    long getCoalmineCapacity();
    void setCoalmineCapacity(long value);

    @IntDefault(10)
    int getCanarySizeThreshold();
    void setCanarySizeThreshold(int value);

    @IntDefault(10)
    int getHesitationMs();
    void setHesitationMs(int value);

    @StringDefault("fsync_async")
    String getDurability();
    void setDurability(String value);

    @StringDefault("leader_quorum")
    String getConsistency();
    void setConsistency(String value);

    @IntDefault(3)
    int getRingSize();
    void setRingSize(int value);

}
