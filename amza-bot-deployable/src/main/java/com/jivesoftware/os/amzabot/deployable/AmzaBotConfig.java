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

}
