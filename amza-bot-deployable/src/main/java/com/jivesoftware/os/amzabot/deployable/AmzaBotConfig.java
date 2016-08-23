package com.jivesoftware.os.amzabot.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

interface AmzaBotConfig extends Config {

    @LongDefault(10_000)
    long getAmzaAwaitLeaderElectionForNMillis();

    @IntDefault(100)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(1_000)
    long getAdditionalSolverAfterNMillis();
    void setAdditionalSolverAfterNMillis(long value);

    @LongDefault(30_000)
    long getAbandonSolutionAfterNMillis();
    void setAbandonSolutionAfterNMillis(long value);

    @BooleanDefault(false)
    boolean getDropEverythingOnTheFloor();
    void setDropEverythingOnTheFloor(boolean value);

    @IntDefault(100)
    int getHesitationFactorMs();
    void setHesitationFactorMs(int value);

    @LongDefault(10_000)
    long getWriteThreshold();
    void setWriteThreshold(long value);

    @IntDefault(100)
    int getValueSizeThreshold();
    void setValueSizeThreshold(int value);

    @IntDefault(3)
    int getPartitionSize();
    void setPartitionSize(int value);

}
