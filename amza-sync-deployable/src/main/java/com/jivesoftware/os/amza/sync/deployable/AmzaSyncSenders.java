package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.OAuthSigner;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import oauth.signpost.signature.HmacSha1MessageSigner;
import org.apache.commons.lang.StringUtils;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public class AmzaSyncSenders {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();


    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, AmzaSyncSender> senders = Maps.newConcurrentMap();

    private final AmzaSyncStats stats;
    private final AmzaSyncConfig syncConfig;
    private final AmzaSyncReceiver syncReceiver;
    private final ScheduledExecutorService executorService;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaClientAquariumProvider clientAquariumProvider;
    private final BAInterner interner;
    private final ObjectMapper mapper;
    private final AmzaSyncSenderConfigProvider syncSenderConfigProvider;
    private final AmzaSyncPartitionConfigProvider syncPartitionConfigProvider;
    private final long ensureSendersInterval;
    private final ExecutorService ensureSenders = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ensure-sender-%d").build());

    public AmzaSyncSenders(AmzaSyncStats stats,
        AmzaSyncConfig syncConfig,
        AmzaSyncReceiver syncReceiver,
        ScheduledExecutorService executorService,
        PartitionClientProvider partitionClientProvider,
        AmzaClientAquariumProvider clientAquariumProvider,
        BAInterner interner,
        ObjectMapper mapper,
        AmzaSyncSenderConfigProvider syncSenderConfigProvider,
        AmzaSyncPartitionConfigProvider syncPartitionConfigProvider,
        long ensureSendersInterval) {
        this.stats = stats;
        this.syncConfig = syncConfig;
        this.syncReceiver = syncReceiver;

        this.executorService = executorService;
        this.partitionClientProvider = partitionClientProvider;
        this.clientAquariumProvider = clientAquariumProvider;
        this.interner = interner;
        this.mapper = mapper;
        this.syncSenderConfigProvider = syncSenderConfigProvider;
        this.syncPartitionConfigProvider = syncPartitionConfigProvider;
        this.ensureSendersInterval = ensureSendersInterval;
    }

    public Collection<String> getSyncspaces() {
        return senders.keySet();
    }

    public Collection<AmzaSyncSender> getActiveSenders() {
        return senders.values();
    }

    public AmzaSyncSender getSender(String syncspaceName) {
        return senders.get(syncspaceName);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            ensureSenders.submit(() -> {
                while (running.get()) {
                    try {
                        Map<String, AmzaSyncSenderConfig> all = syncSenderConfigProvider.getAll();
                        for (Entry<String, AmzaSyncSenderConfig> entry : all.entrySet()) {
                            AmzaSyncSender amzaSyncSender = senders.get(entry.getKey());
                            AmzaSyncSenderConfig senderConfig = entry.getValue();
                            if (amzaSyncSender != null && amzaSyncSender.configHasChanged(senderConfig)) {
                                amzaSyncSender.stop();
                                amzaSyncSender = null;
                            }
                            if (amzaSyncSender == null) {
                                amzaSyncSender = new AmzaSyncSender(
                                    stats,
                                    senderConfig,
                                    clientAquariumProvider,
                                    syncConfig.getSyncSenderRingStripes(),
                                    executorService,
                                    partitionClientProvider,
                                    amzaSyncClient(senderConfig),
                                    mapper,
                                    syncPartitionConfigProvider,
                                    interner
                                );

                                senders.put(entry.getKey(), amzaSyncSender);
                                amzaSyncSender.start();
                            }
                        }

                        // stop any senders that are no longer registered
                        for (Iterator<Entry<String, AmzaSyncSender>> iterator = senders.entrySet().iterator(); iterator.hasNext(); ) {
                            Entry<String, AmzaSyncSender> entry = iterator.next();
                            if (!all.containsKey(entry.getKey())) {
                                entry.getValue().stop();
                                iterator.remove();
                            }
                        }

                        Thread.sleep(ensureSendersInterval);
                    } catch (InterruptedException e) {
                        LOG.info("Ensure senders thread {} was interrupted");
                    } catch (Throwable t) {
                        LOG.error("Failure while ensuring senders", t);
                        Thread.sleep(ensureSendersInterval);
                    }
                }
                return null;
            });
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            ensureSenders.shutdownNow();
        }

        for (AmzaSyncSender amzaSyncSender : senders.values()) {
            try {
                amzaSyncSender.stop();
            } catch (Exception x) {
                LOG.warn("Failure while stopping sender:{}", new Object[] { amzaSyncSender }, x);
            }
        }
    }

    private AmzaSyncClient amzaSyncClient(AmzaSyncSenderConfig config) throws Exception {
        if (config.loopback) {
            return syncReceiver;
        } else {
            String consumerKey = StringUtils.trimToNull(config.oAuthConsumerKey);
            String consumerSecret = StringUtils.trimToNull(config.oAuthConsumerSecret);
            String consumerMethod = StringUtils.trimToNull(config.oAuthConsumerMethod);
            if (consumerKey == null || consumerSecret == null || consumerMethod == null) {
                throw new IllegalStateException("OAuth consumer has not been configured");
            }

            consumerMethod = consumerMethod.toLowerCase();
            if (!consumerMethod.equals("hmac") && !consumerMethod.equals("rsa")) {
                throw new IllegalStateException("OAuth consumer method must be one of HMAC or RSA");
            }

            String scheme = config.senderScheme;
            String host = config.senderHost;
            int port = config.senderPort;

            boolean sslEnable = scheme.equals("https");
            OAuthSigner authSigner = (request) -> {
                CommonsHttpOAuthConsumer oAuthConsumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
                oAuthConsumer.setMessageSigner(new HmacSha1MessageSigner());
                oAuthConsumer.setTokenWithSecret(consumerKey, consumerSecret);
                return oAuthConsumer.sign(request);
            };
            HttpClient httpClient = HttpRequestHelperUtils.buildHttpClient(sslEnable,
                config.allowSelfSignedCerts,
                authSigner,
                host,
                port,
                syncConfig.getSyncSenderSocketTimeout());

            return new HttpAmzaSyncClient(httpClient,
                mapper,
                "/api/sync/v1/commit/rows",
                "/api/sync/v1/ensure/partition");
        }
    }
}
