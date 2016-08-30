package com.jivesoftware.os.amzabot.deployable.endpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineConfig;
import org.merlin.config.BindInterfaceToConfiguration;

class AmzaBotCoalmineRequest {

    private final long coalmineCapacity;
    private final int canarySizeThreshold;
    private final int hesitationMs;
    private final String durability;
    private final String consistency;
    private final int partitionSize;

    @JsonCreator
    public AmzaBotCoalmineRequest(
        @JsonProperty("coalmineCapacity") long coalmineCapacity,
        @JsonProperty("canarySizeThreshold") int canarySizeThreshold,
        @JsonProperty("hesitationMs") int hesitationMs,
        @JsonProperty("durability") String durability,
        @JsonProperty("consistency") String consistency,
        @JsonProperty("partitionSize") int partitionSize) {
        this.coalmineCapacity = coalmineCapacity;
        this.canarySizeThreshold = canarySizeThreshold;
        this.hesitationMs = hesitationMs;
        this.durability = durability;
        this.consistency = consistency;
        this.partitionSize = partitionSize;
    }

    public AmzaBotCoalmineConfig genConfig() {
        AmzaBotCoalmineConfig res =
            BindInterfaceToConfiguration.bindDefault(AmzaBotCoalmineConfig.class);

        if (coalmineCapacity > 0) {
            res.setCoalmineCapacity(coalmineCapacity);
        }

        if (canarySizeThreshold > 0) {
            res.setCanarySizeThreshold(canarySizeThreshold);
        }

        res.setHesitationMs(hesitationMs);

        if (durability != null) {
            res.setDurability(durability);
        }

        if (consistency != null) {
            res.setConsistency(consistency);
        }

        if (partitionSize > 0) {
            res.setPartitionSize(partitionSize);
        }

        return res;
    }

}
