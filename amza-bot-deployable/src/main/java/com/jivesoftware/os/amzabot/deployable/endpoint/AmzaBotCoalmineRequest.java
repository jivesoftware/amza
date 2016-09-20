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
    private final int ringSize;

    @JsonCreator
    public AmzaBotCoalmineRequest(
        @JsonProperty("coalmineCapacity") long coalmineCapacity,
        @JsonProperty("canarySizeThreshold") int canarySizeThreshold,
        @JsonProperty("hesitationMs") int hesitationMs,
        @JsonProperty("durability") String durability,
        @JsonProperty("consistency") String consistency,
        @JsonProperty("ringSize") int ringSize) {
        this.coalmineCapacity = coalmineCapacity;
        this.canarySizeThreshold = canarySizeThreshold;
        this.hesitationMs = hesitationMs;
        this.durability = durability;
        this.consistency = consistency;
        this.ringSize = ringSize;
    }

    static AmzaBotCoalmineConfig genConfig(AmzaBotCoalmineRequest request) {
        AmzaBotCoalmineConfig res =
            BindInterfaceToConfiguration.bindDefault(AmzaBotCoalmineConfig.class);

        if (request != null) {
            if (request.coalmineCapacity > 0) {
                res.setCoalmineCapacity(request.coalmineCapacity);
            }

            if (request.canarySizeThreshold > 0) {
                res.setCanarySizeThreshold(request.canarySizeThreshold);
            }

            res.setHesitationMs(request.hesitationMs);

            if (request.durability != null) {
                res.setDurability(request.durability);
            }

            if (request.consistency != null) {
                res.setConsistency(request.consistency);
            }

            if (request.ringSize > 0) {
                res.setRingSize(request.ringSize);
            }
        }

        return res;
    }

}
