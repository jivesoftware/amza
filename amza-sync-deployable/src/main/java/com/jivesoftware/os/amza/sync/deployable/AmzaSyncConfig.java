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

    @BooleanDefault(false)
    boolean getSyncReceiverUseSolutionLog();

    @BooleanDefault(false)
    boolean getAquariumUseSolutionLog();

    @IntDefault(24)
    int getSyncSendersThreadCount();

    @IntDefault(128)
    int getSyncSenderRingStripes();

    @IntDefault(16)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(60_000)
    long getAmzaAwaitLeaderElectionForNMillis();
}
