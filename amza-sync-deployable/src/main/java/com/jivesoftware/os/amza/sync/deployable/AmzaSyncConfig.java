package com.jivesoftware.os.amza.sync.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface AmzaSyncConfig extends Config {

    @BooleanDefault(false)
    boolean getSyncSenderEnabled();

    @IntDefault(60_000)
    int getSyncSenderSocketTimeout();

    @BooleanDefault(false)
    boolean getSyncReceiverEnabled();

    @IntDefault(16)
    int getSyncRingStripes();

    @IntDefault(4)
    int getSyncThreadCount();

    @IntDefault(16)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(60_000)
    long getAmzaAwaitLeaderElectionForNMillis();

    @BooleanDefault(false)
    boolean getUseClientSolutionLog();
}
