package com.jivesoftware.os.amza.lab.pointers;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;

public interface LABPointerIndexConfig extends Config {

    @IntDefault(8)
    int getMinMergeDebt();

    @IntDefault(16)
    int getMaxMergeDebt();

    @BooleanDefault(false)
    boolean getUseMemMap();

    @IntDefault(1000000)
    int getMaxUpdatesBeforeFlush();

}
