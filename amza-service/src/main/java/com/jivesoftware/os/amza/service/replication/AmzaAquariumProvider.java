package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.AquariumTransactor;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.AmzaPartitionCommitable;
import com.jivesoftware.os.amza.service.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.AwaitNotify;
import com.jivesoftware.os.amza.service.PartitionIsDisposedException;
import com.jivesoftware.os.amza.service.PartitionIsExpungedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.Aquarium;
import com.jivesoftware.os.aquarium.Aquarium.Tx;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.ReadWaterline;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.aquarium.interfaces.AtQuorum;
import com.jivesoftware.os.aquarium.interfaces.AwaitLivelyEndState;
import com.jivesoftware.os.aquarium.interfaces.CurrentMembers;
import com.jivesoftware.os.aquarium.interfaces.LivelinessStorage;
import com.jivesoftware.os.aquarium.interfaces.MemberLifecycle;
import com.jivesoftware.os.aquarium.interfaces.TransitionQuorum;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class AmzaAquariumProvider implements AquariumTransactor, TakeCoordinator.BootstrapPartitions, RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;

    private final AquariumStats aquariumStats;
    private final BAInterner interner;
    private final RingMember rootRingMember;
    private final Member rootAquariumMember;
    private final OrderIdProvider orderIdProvider;
    private final AmzaRingStoreReader ringStoreReader;
    private final SystemWALStorage systemWALStorage;
    private final StorageVersionProvider storageVersionProvider;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final TakeCoordinator takeCoordinator;
    private final WALUpdated walUpdated;
    private final Liveliness liveliness;
    private final long feedEveryMillis;
    private final AwaitNotify<PartitionName> awaitLivelyEndState;
    private final SickThreads sickThreads;

    private final ConcurrentMap<VersionedPartitionName, Aquarium> aquariums = Maps.newConcurrentMap();
    private final ExecutorService livelynessExecutorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("aquarium-livelyness-%d").build());
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("aquarium-scheduled-%d").build());
    private final Set<PartitionName> smellsFishy = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final AtomicLong smellOVersion = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();

    private final Map<VersionedPartitionName, LeadershipTokenAndTookFully> tookFullyWhileNominated = Maps.newConcurrentMap();
    private final Map<VersionedPartitionName, LeadershipTokenAndTookFully> tookFullyWhileInactive = Maps.newConcurrentMap();
    private final Map<VersionedPartitionName, LeadershipTokenAndTookFully> tookFullyWhileBootstrap = Maps.newConcurrentMap();

    public AmzaAquariumProvider(AquariumStats aquariumStats,
        BAInterner interner,
        RingMember rootRingMember,
        OrderIdProvider orderIdProvider,
        AmzaRingStoreReader ringStoreReader,
        SystemWALStorage systemWALStorage,
        StorageVersionProvider storageVersionProvider,
        VersionedPartitionProvider versionedPartitionProvider,
        TakeCoordinator takeCoordinator,
        WALUpdated walUpdated,
        Liveliness liveliness,
        long feedEveryMillis,
        AwaitNotify<PartitionName> awaitLivelyEndState,
        SickThreads sickThreads) {

        this.aquariumStats = aquariumStats;
        this.interner = interner;
        this.rootRingMember = rootRingMember;
        this.rootAquariumMember = rootRingMember.asAquariumMember();
        this.orderIdProvider = orderIdProvider;
        this.ringStoreReader = ringStoreReader;
        this.systemWALStorage = systemWALStorage;
        this.storageVersionProvider = storageVersionProvider;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.takeCoordinator = takeCoordinator;
        this.walUpdated = walUpdated;
        this.liveliness = liveliness;
        this.feedEveryMillis = feedEveryMillis;
        this.awaitLivelyEndState = awaitLivelyEndState;
        this.sickThreads = sickThreads;
    }

    public void start() {

        running.set(true);
        livelynessExecutorService.submit(() -> {
            while (running.get()) {
                try {
                    liveliness.feedTheFish();
                    sickThreads.recovered();
                    Thread.sleep(feedEveryMillis);
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable t) {
                    sickThreads.sick(t);
                    LOG.error("Failed to feed the fish", t);
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            sickThreads.recovered();
            return null;
        });

        executorService.submit(() -> {
            while (running.get()) {
                try {
                    long startVersion = smellOVersion.get();
                    Iterator<PartitionName> iter = smellsFishy.iterator();
                    while (iter.hasNext()) {
                        PartitionName partitionName = iter.next();
                        iter.remove();
                        try {
                            if (ringStoreReader.isMemberOfRing(partitionName.getRingName(), 0)) {

                                StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
                                if (storageVersion != null) {
                                    VersionedPartitionName versionedPartitionName = storageVersionProvider.tx(partitionName, storageVersion,
                                        (deltaIndex, stripeIndex, storageVersion1) -> new VersionedPartitionName(partitionName,
                                            storageVersion1.partitionVersion));

                                    Aquarium aquarium = getAquarium(versionedPartitionName);
                                    aquarium.acknowledgeOther();
                                    aquarium.tapTheGlass();
                                    takeCoordinator.stateChanged(ringStoreReader, versionedPartitionName);
                                }
                            }
                        } catch (PartitionIsDisposedException e) {
                            LOG.info("Ignored disposed partition {}", partitionName);
                        } catch (PropertiesNotPresentException | IllegalArgumentException e) {
                            // somewhat expected
                            smellsFishy.add(partitionName);
                        } catch (Exception e) {
                            smellsFishy.add(partitionName);
                            throw e;
                        }
                    }
                    sickThreads.recovered();
                    synchronized (smellsFishy) {
                        if (startVersion == smellOVersion.get()) {
                            smellsFishy.wait(smellsFishy.isEmpty() ? 0 : feedEveryMillis);
                        }
                    }
                    /*LOG.info("Smelled {} fish in {}", count, (System.currentTimeMillis() - start));*/
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable t) {
                    sickThreads.sick(t);
                    LOG.error("Failed to feed the fish", t);
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            sickThreads.recovered();
            return null;
        });
    }

    public void stop() {
        running.set(false);
        livelynessExecutorService.shutdownNow();
        executorService.shutdownNow();
    }

    public <R> R tx(VersionedPartitionName versionedPartitionName, Tx<R> tx) throws Exception {
        return getAquarium(versionedPartitionName).tx(tx);
    }

    @Override
    public void tookFully(VersionedPartitionName versionedPartitionName, RingMember fromRingMember, long leadershipToken) throws Exception {
        Aquarium aquarium = getAquarium(versionedPartitionName);
        Waterline leader = aquarium.getLeader();
        Waterline currentWaterline = aquarium.getState(rootAquariumMember);
        if (leader != null
            && leadershipToken == leader.getTimestamp()
            && rootAquariumMember.equals(leader.getMember())
            && currentWaterline.isAtQuorum() && currentWaterline.getState() == State.nominated) {

            tookFullyWhileNominated.compute(versionedPartitionName, (key, current) -> {
                if (current == null || current.leadershipToken < leadershipToken) {
                    current = new LeadershipTokenAndTookFully(leadershipToken);
                }
                current.add(fromRingMember);
                return current;
            });
        } else {
            tookFullyWhileNominated.remove(versionedPartitionName);
        }

        if (leader != null
            && leadershipToken == leader.getTimestamp()
            && currentWaterline.isAtQuorum() && currentWaterline.getState() == State.inactive) {

            tookFullyWhileInactive.compute(versionedPartitionName, (key, current) -> {
                if (current == null || current.leadershipToken < leadershipToken) {
                    current = new LeadershipTokenAndTookFully(leadershipToken);
                }
                current.add(fromRingMember);
                return current;
            });
        } else {
            tookFullyWhileInactive.remove(versionedPartitionName);
        }

        //if (!versionedPartitionName.getPartitionName().isSystemPartition()) LOG.info("TOOK FULLY +++ {}={} from {} in {} at {}",
        // rootRingMember, currentWaterline.getState(), fromRingMember, versionedPartitionName, leadershipToken);
        if (currentWaterline.getState() == State.bootstrap && (leader == null || leadershipToken == leader.getTimestamp())) {
            tookFullyWhileBootstrap.compute(versionedPartitionName, (key, current) -> {
                if (current == null || current.leadershipToken < leadershipToken) {
                    current = new LeadershipTokenAndTookFully(leadershipToken);
                }
                current.add(fromRingMember);
                return current;
            });
        } else {
            tookFullyWhileBootstrap.remove(versionedPartitionName);
        }
    }

    @Override
    public boolean isColdstart(VersionedPartitionName versionedPartitionName) throws Exception {
        RingTopology ring = ringStoreReader.getRing(versionedPartitionName.getPartitionName().getRingName(), 0);
        int quorumCount = (ring.entries.size() + 1) / 2;

        Aquarium aquarium = getAquarium(versionedPartitionName);
        int bootstrapCount = 0;
        for (RingMemberAndHost ringMemberAndHost : ring.entries) {
            Waterline waterline = aquarium.getState(ringMemberAndHost.ringMember.asAquariumMember());
            if (waterline.getState() == State.bootstrap) {
                bootstrapCount++;
                if (bootstrapCount >= quorumCount) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isMemberInState(VersionedPartitionName versionedPartitionName, RingMember ringMember, State state) throws Exception {
        Aquarium aquarium = getAquarium(versionedPartitionName);
        return state == aquarium.getState(ringMember.asAquariumMember()).getState();
    }

    private Aquarium getAquarium(VersionedPartitionName versionedPartitionName) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (versionedPartitionProvider.getProperties(partitionName) == null) {
            throw new PropertiesNotPresentException("Properties missing for " + partitionName);
        }

        return aquariums.computeIfAbsent(versionedPartitionName, key -> {
            try {
                return buildAquarium(key);
            } catch (Exception e) {
                throw new RuntimeException("Failed to build aquarium for partition " + versionedPartitionName, e);
            }
        });
    }

    public Waterline getCurrentState(PartitionName partitionName, RingMember remoteRingMember, long remotePartitionVersion) throws Exception {
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, remotePartitionVersion);
        AmzaStateStorage amzaStateStorage = currentStateStorage(partitionName);
        AmzaMemberLifecycle amzaMemberLifecycle = new AmzaMemberLifecycle(storageVersionProvider, versionedPartitionName, rootAquariumMember);
        AmzaAtQuorum atQuorum = new AmzaAtQuorum(versionedPartitionProvider, ringStoreReader, versionedPartitionName);
        CurrentMembers currentMembers = () -> {
            return ringStoreReader.getRing(partitionName.getRingName(), 0).aquariumMembers;
        };
        return new ReadWaterline<>(aquariumStats.getMyCurrentWaterline, aquariumStats.getOthersCurrentWaterline, aquariumStats.acknowledgeCurrentOther,
            amzaStateStorage, amzaMemberLifecycle, atQuorum, currentMembers, Long.class).get(remoteRingMember.asAquariumMember());
    }

    private Aquarium buildAquarium(VersionedPartitionName versionedPartitionName) throws Exception {
        AtQuorum atQuorum = new AmzaAtQuorum(versionedPartitionProvider, ringStoreReader, versionedPartitionName);

        AmzaMemberLifecycle memberLifecycle = new AmzaMemberLifecycle(storageVersionProvider, versionedPartitionName, rootAquariumMember);

        TransitionQuorum currentTransitionQuorum = (existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired) -> {
            PartitionProperties properties = versionedPartitionProvider.getProperties(versionedPartitionName.getPartitionName());
            if (existing.getState() == State.nominated && nextState == State.leader) {
                int ringSize = ringStoreReader.getRingSize(versionedPartitionName.getPartitionName().getRingName(), 0);
                int quorum;
                boolean needsRepair;
                if (properties.consistency == Consistency.leader) {
                    // taking from demoted is the only way to repair
                    quorum = Integer.MAX_VALUE;
                    needsRepair = true;
                } else {
                    quorum = properties.consistency.repairQuorum(ringSize - 1);
                    needsRepair = quorum > 0;
                }
                if (needsRepair) {
                    LeadershipTokenAndTookFully leadershipTokenAndTookFully = tookFullyWhileNominated.get(versionedPartitionName);
                    boolean repairedFromDemoted = false;
                    if (leadershipTokenAndTookFully != null
                        && !leadershipTokenAndTookFully.tookFully.isEmpty()
                        && properties.consistency.requiresLeader()) {
                        Waterline demoted = State.highest(rootAquariumMember, State.demoted, readCurrent, existing);
                        if (demoted != null
                            && demoted.isAtQuorum()
                            && liveliness.isAlive(demoted.getMember())
                            && leadershipTokenAndTookFully.tookFully.contains(RingMember.fromAquariumMember(demoted.getMember()))) {
                            repairedFromDemoted = true;
                        }
                    }
                    if (leadershipTokenAndTookFully == null
                        || leadershipTokenAndTookFully.leadershipToken != nextTimestamp
                        || (leadershipTokenAndTookFully.tookFully() < quorum && !repairedFromDemoted)) {
                        LOG.info("{} is nominated for version {} and has taken fully {} out of {}: {}", versionedPartitionName,
                            leadershipTokenAndTookFully == null ? 0 : leadershipTokenAndTookFully.leadershipToken,
                            leadershipTokenAndTookFully == null ? 0 : leadershipTokenAndTookFully.tookFully(),
                            quorum,
                            leadershipTokenAndTookFully == null ? Collections.emptyList() : leadershipTokenAndTookFully.tookFully);
                        return false;
                    } else if (repairedFromDemoted) {
                        LOG.info("{} is nominated for version {} and has taken fully from the demoted member.", versionedPartitionName,
                            leadershipTokenAndTookFully.leadershipToken);
                    } else {
                        LOG.info("{} is nominated for version {} and has taken fully {} out of {}.", versionedPartitionName,
                            leadershipTokenAndTookFully.leadershipToken, leadershipTokenAndTookFully.tookFully(), quorum);
                    }
                } else {
                    LOG.info("{} is nominated and does not need repair.", versionedPartitionName);
                }
            } else if (existing.getState() == State.inactive && nextState == State.follower
                || existing.getState() == State.bootstrap && nextState == State.inactive) {

                Map<VersionedPartitionName, LeadershipTokenAndTookFully> tookFullyInCurrentState =
                    existing.getState() == State.inactive ? tookFullyWhileInactive : tookFullyWhileBootstrap;

                int ringSize = ringStoreReader.getRingSize(versionedPartitionName.getPartitionName().getRingName(), 0);
                int quorum;
                boolean needsRepair;
                if (properties.consistency == Consistency.leader) {
                    // taking from leader is the only way to repair
                    quorum = Integer.MAX_VALUE;
                    needsRepair = true;
                } else {
                    quorum = properties.consistency.repairQuorum(ringSize - 1);
                    needsRepair = quorum > 0;
                }
                if (needsRepair) {
                    LeadershipTokenAndTookFully leadershipTokenAndTookFully = tookFullyInCurrentState.get(versionedPartitionName);
                    Waterline leader = State.highest(rootAquariumMember, State.leader, readDesired, readDesired.get(rootAquariumMember));
                    long leaderToken = leader != null ? leader.getTimestamp() : -1;
                    boolean repairedFromLeader = false;
                    if (leader != null
                        && leadershipTokenAndTookFully != null
                        && properties.consistency.requiresLeader()
                        && leader.getTimestamp() == leadershipTokenAndTookFully.leadershipToken
                        && leader.isAtQuorum()
                        && liveliness.isAlive(leader.getMember())
                        && leadershipTokenAndTookFully.tookFully.contains(RingMember.fromAquariumMember(leader.getMember()))) {
                        repairedFromLeader = true;
                    }
                    if (leader == null
                        || leadershipTokenAndTookFully == null
                        || leadershipTokenAndTookFully.leadershipToken != leader.getTimestamp()
                        || (leadershipTokenAndTookFully.tookFully() < quorum && !repairedFromLeader)) {
                        LOG.info("{} {} is {} for version {}/{} and has taken fully {} out of {}.", rootRingMember,
                            versionedPartitionName,
                            existing.getState(),
                            leadershipTokenAndTookFully == null ? -1 : leadershipTokenAndTookFully.leadershipToken,
                            leaderToken,
                            leadershipTokenAndTookFully == null ? 0 : leadershipTokenAndTookFully.tookFully(),
                            quorum);
                        return false;
                    } else if (repairedFromLeader) {
                        LOG.info("{} {} is {} for version {}/{} and has taken fully from the leader.", rootRingMember,
                            versionedPartitionName,
                            existing.getState(),
                            leadershipTokenAndTookFully.leadershipToken,
                            leaderToken);
                    } else {
                        LOG.info("{} {} is {} for version {}/{} and has taken fully {} out of {}.", rootRingMember,
                            versionedPartitionName,
                            existing.getState(),
                            leadershipTokenAndTookFully.leadershipToken,
                            leaderToken,
                            leadershipTokenAndTookFully.tookFully(),
                            quorum);
                    }
                } else {
                    LOG.info("{} is {} and does not need repair.", versionedPartitionName, existing.getState());
                }
            }

            return writeCurrent.put(rootAquariumMember, nextState, nextTimestamp);
        };

        TransitionQuorum desiredTransitionQuorum = (existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired) -> {
            return writeDesired.put(rootAquariumMember, nextState, nextTimestamp);
        };
        CurrentMembers currentMembers = () -> {
            return ringStoreReader.getRing(versionedPartitionName.getPartitionName().getRingName(), 0).aquariumMembers;
        };

        return new Aquarium(aquariumStats,
            orderIdProvider,
            currentStateStorage(versionedPartitionName.getPartitionName()),
            desiredStateStorage(versionedPartitionName.getPartitionName()),
            currentTransitionQuorum,
            desiredTransitionQuorum,
            liveliness,
            memberLifecycle,
            Long.class,
            atQuorum,
            currentMembers,
            rootRingMember.asAquariumMember(),
            new AwaitLivelyEndState() {
            @Override
            public LivelyEndState awaitChange(Callable<LivelyEndState> awaiter, long timeoutMillis) throws Exception {
                return awaitLivelyEndState.awaitChange(versionedPartitionName.getPartitionName(),
                    () -> {
                        LivelyEndState state = awaiter.call();
                        return state != null ? Optional.of(state) : null;
                    },
                    timeoutMillis);
            }

            @Override
            public void notifyChange(Callable<Boolean> change) throws Exception {
                awaitLivelyEndState.notifyChange(versionedPartitionName.getPartitionName(), change);
            }
        });
    }

    @Override
    public boolean bootstrap(TakeCoordinator.PartitionStream partitionStream) throws Exception {
        for (Map.Entry<VersionedPartitionName, Aquarium> entry : aquariums.entrySet()) {
            VersionedPartitionName versionedPartitionName = entry.getKey();
            LivelyEndState livelyEndState = entry.getValue().livelyEndState();
            if (!livelyEndState.isOnline()) {
                if (!partitionStream.stream(versionedPartitionName, livelyEndState)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public LivelyEndState getLivelyEndState(VersionedPartitionName versionedPartitionName) throws Exception {
        return getAquarium(versionedPartitionName).livelyEndState();
    }

    @Override
    public boolean isLivelyEndState(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        return getAquarium(versionedPartitionName).isLivelyEndState(ringMember.asAquariumMember());
    }

    @Override
    public void wipeTheGlass(VersionedPartitionName versionedPartitionName) throws Exception {
        LivelyEndState livelyEndState = getLivelyEndState(versionedPartitionName);
        if (livelyEndState.getCurrentState() == State.expunged) {
            throw new PartitionIsExpungedException("Partition " + versionedPartitionName + " is expunged");
        } else if ((!livelyEndState.isOnline() || !isOnline(livelyEndState.getLeaderWaterline()))
            && ringStoreReader.isMemberOfRing(versionedPartitionName.getPartitionName().getRingName(), 0)) {
            getAquarium(versionedPartitionName).tapTheGlass();
        }
    }

    @Override
    public boolean suggestState(VersionedPartitionName versionedPartitionName, State state) throws Exception {
        return getAquarium(versionedPartitionName).suggestState(state);
    }

    public boolean isOnline(Waterline waterline) throws Exception {
        return waterline != null && waterline.isAtQuorum() && liveliness.isAlive(waterline.getMember());
    }

    @Override
    public LivelyEndState awaitOnline(VersionedPartitionName versionedPartitionName, long timeoutMillis) throws Exception {
        Aquarium aquarium = getAquarium(versionedPartitionName);
        LivelyEndState livelyEndState = aquarium.livelyEndState();
        if (!livelyEndState.isOnline()) {
            if (ringStoreReader.isMemberOfRing(versionedPartitionName.getPartitionName().getRingName(), 0)) {
                Waterline leader = livelyEndState.getLeaderWaterline();
                if (leader == null || !leader.getMember().equals(rootAquariumMember)) {

                    aquarium.suggestState(State.follower);
                }
                aquarium.tapTheGlass();
            }
        }
        return aquarium.awaitOnline(timeoutMillis);
    }

    @Override
    public Waterline getLeader(VersionedPartitionName versionedPartitionName) throws Exception {
        return getAquarium(versionedPartitionName).getLeader();
    }

    public Waterline remoteAwaitProbableLeader(PartitionName partitionName, long timeoutMillis) throws Exception {
        AmzaStateStorage amzaStateStorage = currentStateStorage(partitionName);

        Waterline[] leader = new Waterline[1];
        long doneAfterTimestamp = System.currentTimeMillis() + timeoutMillis;
        while (leader[0] == null) {
            amzaStateStorage.scan(null, null, null, (rootMember, isSelf, ackMember, lifecycle, state, timestamp, version) -> {
                if (state == State.leader) {
                    if (rootMember.equals(ackMember)) {
                        Waterline member = new Waterline(rootMember, state, timestamp, version, false);
                        if (leader[0] == null || Waterline.compare(leader[0], member) > 0) {
                            leader[0] = member;
                        }
                    }
                }
                return true;
            });
            if (leader[0] == null && System.currentTimeMillis() < doneAfterTimestamp) {
                long timeToWait = Math.min(100, doneAfterTimestamp - System.currentTimeMillis()); //TODO magic number
                Thread.sleep(timeToWait);
            } else {
                break;
            }
        }

        return leader[0];
    }

    @Override
    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        long partitionVersion = versionedPartitionName.getPartitionVersion();

        AmzaStateStorage currentStateStorage = currentStateStorage(partitionName);
        AmzaStateStorage desiredStateStorage = desiredStateStorage(partitionName);

        AmzaPartitionUpdates amzaPartitionUpdates = new AmzaPartitionUpdates();
        currentStateStorage.scan(rootAquariumMember, null, partitionVersion, (rootMember1, isSelf, ackMember, lifecycle1, state, timestamp, version) -> {
            byte[] keyBytes = AmzaAquariumProvider.stateKey(partitionName, CURRENT, rootMember1, lifecycle1, ackMember);
            amzaPartitionUpdates.remove(keyBytes, timestamp);
            return true;
        });
        desiredStateStorage.scan(rootAquariumMember, null, partitionVersion, (rootMember1, isSelf, ackMember, lifecycle1, state, timestamp, version) -> {
            byte[] keyBytes = AmzaAquariumProvider.stateKey(partitionName, DESIRED, rootMember1, lifecycle1, ackMember);
            amzaPartitionUpdates.remove(keyBytes, timestamp);
            return true;
        });
        systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, new AmzaPartitionCommitable(amzaPartitionUpdates, orderIdProvider), walUpdated);
    }

    private AmzaStateStorage currentStateStorage(PartitionName partitionName) {
        return new AmzaStateStorage(interner, systemWALStorage, orderIdProvider, walUpdated, partitionName, CURRENT);
    }

    private AmzaStateStorage desiredStateStorage(PartitionName partitionName) {
        return new AmzaStateStorage(interner, systemWALStorage, orderIdProvider, walUpdated, partitionName, DESIRED);
    }

    static byte[] stateKey(PartitionName partitionName,
        byte context,
        Member rootRingMember,
        Long partitionVersion,
        Member ackRingMember) throws Exception {
        byte[] lengthBuffer = new byte[4];
        int partitionSizeInBytes = 4 + partitionName.sizeInBytes();
        if (rootRingMember != null && ackRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            int ackSizeInBytes = 4 + ackRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + rootSizeInBytes + 8 + 1 + ackSizeInBytes);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName", lengthBuffer);
            filer.write(new byte[]{context}, 0, 1);
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember", lengthBuffer);
            UIO.writeLong(filer, partitionVersion, "partitionVersion");
            UIO.write(filer, !rootRingMember.equals(ackRingMember) ? new byte[]{(byte) 1} : new byte[]{(byte) 0}, "isOther");
            UIO.writeByteArray(filer, ackRingMember.getMember(), "ackRingMember", lengthBuffer);
            return filer.getBytes();
        } else if (rootRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + rootSizeInBytes + 8);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName", lengthBuffer);
            filer.write(new byte[]{context}, 0, 1);
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember", lengthBuffer);
            UIO.writeLong(filer, partitionVersion, "partitionVersion");
            return filer.getBytes();
        } else {
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName", lengthBuffer);
            filer.write(new byte[]{context}, 0, 1);
            return filer.getBytes();
        }
    }

    static boolean streamStateKey(byte[] keyBytes, StateKeyStream stream) throws Exception {
        int o = 0;
        int partitionNameBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        PartitionName partitionName = PartitionName.fromBytes(keyBytes, o);
        o += partitionNameBytesLength;
        byte context = keyBytes[o];
        o++;
        int rootRingMemberBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        byte[] rootRingMemberBytes = copy(keyBytes, o, rootRingMemberBytesLength);
        o += rootRingMemberBytesLength;
        long partitionVersion = UIO.bytesLong(keyBytes, o);
        o += 8;
        boolean isOther = keyBytes[o] != 0;
        boolean isSelf = !isOther;
        o++;
        int ackRingMemberBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        byte[] ackRingMemberBytes = copy(keyBytes, o, ackRingMemberBytesLength);
        o += ackRingMemberBytesLength;

        return stream.stream(partitionName,
            context,
            new Member(rootRingMemberBytes),
            partitionVersion,
            isSelf,
            new Member(ackRingMemberBytes));
    }

    static private byte[] copy(byte[] bytes,int offest,int length) {
        byte[] copy = new byte[length];
        System.arraycopy(bytes,offest,copy,0,length);
        return copy;
    }

    interface StateKeyStream {

        boolean stream(PartitionName partitionName,
            byte context,
            Member rootRingMember,
            long partitionVersion,
            boolean isSelf,
            Member ackRingMember) throws Exception;
    }

    private static byte[] livelinessKey(Member rootRingMember, Member ackRingMember) throws Exception {
        Preconditions.checkNotNull(rootRingMember, "Requires root ring member");
        byte[] lengthBuffer = new byte[4];
        if (ackRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            int ackSizeInBytes = 4 + ackRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(rootSizeInBytes + 1 + ackSizeInBytes);
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember", lengthBuffer);
            UIO.write(filer, !rootRingMember.equals(ackRingMember) ? new byte[]{(byte) 1} : new byte[]{(byte) 0}, "isOther");
            UIO.writeByteArray(filer, ackRingMember.getMember(), "ackRingMember", lengthBuffer);
            return filer.getBytes();
        } else {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(rootSizeInBytes);
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember", lengthBuffer);
            return filer.getBytes();
        }
    }

    private static boolean streamLivelinessKey(byte[] keyBytes, LivelinessKeyStream stream, byte[] intLongBuffer) throws Exception {
        HeapFiler filer = HeapFiler.fromBytes(keyBytes, keyBytes.length);
        byte[] rootRingMemberBytes = UIO.readByteArray(filer, "rootRingMember", intLongBuffer);
        boolean isSelf = !UIO.readBoolean(filer, "isOther");
        byte[] ackRingMemberBytes = UIO.readByteArray(filer, "ackRingMember", intLongBuffer);
        return stream.stream(new Member(rootRingMemberBytes),
            isSelf,
            new Member(ackRingMemberBytes));
    }

    interface LivelinessKeyStream {

        boolean stream(Member rootRingMember, boolean isSelf, Member ackRingMember) throws Exception;
    }

    public static class AmzaLivelinessStorage implements LivelinessStorage {

        private final SystemWALStorage systemWALStorage;
        private final OrderIdProvider orderIdProvider;
        private final WALUpdated walUpdated;
        private final Member member;
        private final long startupVersion;

        public AmzaLivelinessStorage(SystemWALStorage systemWALStorage,
            OrderIdProvider orderIdProvider,
            WALUpdated walUpdated,
            Member member,
            long startupVersion) {
            this.systemWALStorage = systemWALStorage;
            this.orderIdProvider = orderIdProvider;
            this.member = member;
            this.walUpdated = walUpdated;
            this.startupVersion = startupVersion;
        }

        @Override
        public boolean scan(Member rootMember, Member otherMember, LivelinessStream stream) throws Exception {
            byte[] intLongBuffer = new byte[8];

            KeyValueStream keyValueStream = (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    return streamLivelinessKey(key, (rootRingMember, isSelf, ackRingMember) -> {
                        if (!rootRingMember.equals(member) || valueVersion > startupVersion) {
                            return stream.stream(rootRingMember, isSelf, ackRingMember, valueTimestamp, valueVersion);
                        } else {
                            return true;
                        }
                    }, intLongBuffer);
                }
                return true;
            };

            if (rootMember == null && otherMember == null) {
                return systemWALStorage.rowScan(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, keyValueStream, true);
            } else {
                byte[] fromKey = livelinessKey(rootMember, otherMember);
                return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
                    keyValueStream, true);
            }
        }

        @Override
        public boolean update(LivelinessUpdates updates) throws Exception {
            AmzaPartitionUpdates amzaPartitionUpdates = new AmzaPartitionUpdates();
            boolean result = updates.updates((rootMember, otherMember, timestamp) -> {
                byte[] keyBytes = livelinessKey(rootMember, otherMember);
                amzaPartitionUpdates.set(keyBytes, new byte[0], timestamp);
                return true;
            });
            if (result && amzaPartitionUpdates.size() > 0) {
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_LIVELINESS_INDEX,
                    null,
                    new AmzaPartitionCommitable(amzaPartitionUpdates, orderIdProvider),
                    walUpdated);
                return !rowsChanged.isEmpty();
            } else {
                return false;
            }
        }

        @Override
        public long get(Member rootMember, Member otherMember) throws Exception {
            TimestampedValue timestampedValue = systemWALStorage.getTimestampedValue(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, null,
                livelinessKey(rootMember, otherMember));
            return timestampedValue != null ? timestampedValue.getTimestampId() : -1;
        }
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.AQUARIUM_STATE_INDEX.equals(changes.getVersionedPartitionName())) {
            for (Map.Entry<WALKey, WALValue> change : changes.getApply().entrySet()) {
                streamStateKey(change.getKey().key, (partitionName, context, rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
                    smellsFishy.add(partitionName);
                    return true;
                });
            }
            synchronized (smellsFishy) {
                smellOVersion.incrementAndGet();
                smellsFishy.notifyAll();
            }
        }
    }

    private static class AmzaMemberLifecycle implements MemberLifecycle<Long> {

        private final StorageVersionProvider storageVersionProvider;
        private final VersionedPartitionName versionedPartitionName;
        private final Member rootMember;

        public AmzaMemberLifecycle(StorageVersionProvider storageVersionProvider, VersionedPartitionName versionedPartitionName, Member rootMember) {
            this.storageVersionProvider = storageVersionProvider;
            this.versionedPartitionName = versionedPartitionName;
            this.rootMember = rootMember;
        }

        @Override
        public Long get(Member member) throws Exception {
            if (member.equals(rootMember)) {
                return versionedPartitionName.getPartitionVersion();
            } else {
                StorageVersion storageVersion = storageVersionProvider.getRemote(RingMember.fromAquariumMember(member),
                    versionedPartitionName.getPartitionName());
                return storageVersion != null ? storageVersion.partitionVersion : null;
            }
        }
    }

    private static class AmzaAtQuorum implements AtQuorum {

        private final VersionedPartitionProvider versionedPartitionProvider;
        private final AmzaRingReader amzaRingReader;
        private final VersionedPartitionName versionedPartitionName;

        public AmzaAtQuorum(VersionedPartitionProvider versionedPartitionProvider,
            AmzaRingReader amzaRingReader,
            VersionedPartitionName versionedPartitionName) {

            this.versionedPartitionProvider = versionedPartitionProvider;
            this.amzaRingReader = amzaRingReader;
            this.versionedPartitionName = versionedPartitionName;
        }

        @Override
        public boolean is(int count) throws Exception {
            PartitionProperties properties = versionedPartitionProvider.getProperties(versionedPartitionName.getPartitionName());
            if (properties.replicated) {
                return count > amzaRingReader.getRingSize(versionedPartitionName.getPartitionName().getRingName(), 0) / 2;
            } else {
                return true;
            }
        }
    }

    static class LeadershipTokenAndTookFully {

        final long leadershipToken;
        final Set<RingMember> tookFully = Collections.synchronizedSet(new HashSet<>());

        public LeadershipTokenAndTookFully(long leadershipToken) {
            this.leadershipToken = leadershipToken;
        }

        void add(RingMember ringMember) {
            tookFully.add(ringMember);
        }

        int tookFully() {
            return tookFully.size();
        }

        @Override
        public String toString() {
            return "LeadershipTokenAndTookFully{" + "leadershipToken=" + leadershipToken + ", tookFully=" + tookFully + '}';
        }

    }
}
