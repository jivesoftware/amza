package com.jivesoftware.os.amza.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public class AmzaSyncSenderConfig {


    public final String name;
    public final boolean enabled;
    public final String senderScheme;
    public final String senderHost;
    public final int senderPort;
    public final int senderSocketTimeout;
    public final int ringStripes;
    public final int threadCount;
    public final long syncIntervalMillis;
    public final int batchSize;
    public final long awaitLeaderElectionForNMillis;
    public final String oAuthConsumerKey;
    public final String oAuthConsumerSecret;
    public final String oAuthConsumerMethod;
    public final boolean allowSelfSignedCerts;

    @JsonCreator
    public AmzaSyncSenderConfig(@JsonProperty("name") String name,
        @JsonProperty("enabled") boolean enabled,
        @JsonProperty("senderScheme") String senderScheme,
        @JsonProperty("senderHost")  String senderHost,
        @JsonProperty("senderPort") int senderPort,
        @JsonProperty("senderSocketTimeout") int senderSocketTimeout,
        @JsonProperty("ringStripes") int ringStripes,
        @JsonProperty("threadCount") int threadCount,
        @JsonProperty("syncIntervalMillis") long syncIntervalMillis,
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("awaitLeaderElectionForNMillis") long awaitLeaderElectionForNMillis,
        @JsonProperty("oAuthConsumerKey") String oAuthConsumerKey,
        @JsonProperty("oAuthConsumerSecret") String oAuthConsumerSecret,
        @JsonProperty("oAuthConsumerMethod") String oAuthConsumerMethod,
        @JsonProperty("allowSelfSignedCerts") boolean allowSelfSignedCerts) {

        this.name = name;
        this.enabled = enabled;
        this.senderScheme = senderScheme;
        this.senderHost = senderHost;
        this.senderPort = senderPort;
        this.senderSocketTimeout = senderSocketTimeout;
        this.ringStripes = ringStripes;
        this.threadCount = threadCount;
        this.syncIntervalMillis = syncIntervalMillis;
        this.batchSize = batchSize;
        this.awaitLeaderElectionForNMillis = awaitLeaderElectionForNMillis;
        this.oAuthConsumerKey = oAuthConsumerKey;
        this.oAuthConsumerSecret = oAuthConsumerSecret;
        this.oAuthConsumerMethod = oAuthConsumerMethod;
        this.allowSelfSignedCerts = allowSelfSignedCerts;
    }
}
