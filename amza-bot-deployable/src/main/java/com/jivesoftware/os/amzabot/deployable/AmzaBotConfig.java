package com.jivesoftware.os.amzabot.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface AmzaBotConfig extends Config {

    @LongDefault(10_000L)
    long getAmzaAwaitLeaderElectionForNMillis();

    @IntDefault(100)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(1_000L)
    long getAdditionalSolverAfterNMillis();

    @LongDefault(30_000L)
    long getAbandonSolutionAfterNMillis();

    @LongDefault(30_000L)
    long getAbandonLeaderSolutionAfterNMillis();

    @BooleanDefault(false)
    boolean getDropEverythingOnTheFloor();

    @IntDefault(3)
    int getPartitionSize();
    void setPartitionSize(int value);

}
