/*
 * Copyright 2016 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class POCTakerCornerCases {

    /*
    A-U1-T1,B-U2-T2,C-U3-T3
    A-U1-T1,B-U2-T3,C-U3-T2
    A-U1-T2,B-U2-T3,C-U3-T1
    A-U1-T2,B-U2-T1,C-U3-T3
    A-U1-T3,B-U2-T2,C-U3-T1
    A-U1-T3,B-U2-T1,C-U3-T2


    A1,B2,C3 - [A1,B2,C3], [A1,B2,C3], [A1,B2,C3]
    A1,B2,C3 - [A1,  ,  ], [A1,B2,  ], [  ,B2,  ]
    A1,B2,C3 - [A1,B3,  ], [A1,B2,  ], [  ,B2,  ]

    A1,B3,C2
    A2,B3,C1
    A2,B1,C3
    A3,B2,C1
    A3,B1,C2
     */

 /*
    Workload % Read % Scans % Inserts
R 95 0 5
RW 50 0 50
W 1 0 99
RS 47 47 6
RSW 25 25 50


    (YCSB)

    Our data set consists of records with a single alphanumeric key
with a length of 25 bytes and 5 value fields each with 10 bytes.
Thus, a single record has a raw size of 75 bytes.
     */
    static Set<Long> actualyAdded = Collections.newSetFromMap(new ConcurrentHashMap<>());
    static boolean magicEnabled = true;

    @Test(enabled = true)
    public void takerCornerCases() throws Exception {

        Random rand = new Random(1234);
        ExecutorService executorService = Executors.newCachedThreadPool();

        int nodeCount = 10;
        int numIds = 200;
        int pause = 10;
        int maxId = nodeCount * numIds;

        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            OrderIdProvider idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(i));
            long stbAfterNAdds = (i % 2 == 0) ? 100 : Long.MAX_VALUE;
            nodes.add(new Node(rand, idProvider, "node-" + i, 0, i, nodeCount, maxId, pause, stbAfterNAdds, nodes));
        }

        List<Future> futures = new ArrayList<>();
        for (Node node : nodes) {
            futures.add(executorService.submit(node));
        }

        Set<NodeKey> validated = new HashSet<>();
        startTakingThread("all", executorService, validated, nodes, maxId);

        for (Future future : futures) {
            future.get();
        }

        for (Node node : nodes) {
            node.walEntryStreamed = 0;
            node.walHighwaterStreamed = 0;
        }

        Node node = nodes.get(0);
        node.clear();
        node.version = 1;

        validated.clear();
        startTakingThread("lose a node", executorService, validated, nodes, maxId);

    }

    private void startTakingThread(String phaseName, ExecutorService executorService, Set<NodeKey> validated, List<Node> nodes, int maxId) throws Exception {
        Future<?> takingFuture = executorService.submit(() -> {
            int invalid = nodes.size();
            while (invalid > 0) {
                try {
                    invalid = 0;
                    for (Node node : nodes) {
                        if (!validated.contains(new NodeKey(node.name, node.version))) {
                            invalid++;
                        }
                        node.take();
                    }
                    Thread.sleep(10);
                } catch (Exception x) {
                    x.printStackTrace();
                }
            }
        });

        while (validated.size() < nodes.size()) {
            for (Node node : nodes) {
                if (node.isValid(maxId)) {
                    validated.add(new NodeKey(node.name, node.version));
                }
            }
            System.out.println("---------------------------------------------");
            System.out.println("----------- " + phaseName + "----------------");
            System.out.println("---------------------------------------------");
            Thread.sleep(1000);
        }

        takingFuture.get();
    }

    static class Node implements Callable, TakeStream {

        ConcurrentHashMap<Long, Long> index = new ConcurrentHashMap<>();
        final ConcurrentNavigableMap<Long, Integer> txIdToWalIndex = new ConcurrentSkipListMap<>();
        final List<Object> wal = new ArrayList<>();

        Map<NodeKey, Long> highwaters = new ConcurrentSkipListMap();

        Random rand;
        OrderIdProvider txIdProvider;

        String name;
        long version;
        long start;
        long step;
        long stop;
        long stbAfterNAdds;
        long pauseBetweenAdds;

        long added = 0;
        long walEntryStreamed = 0;
        long walHighwaterStreamed = 0;

        long highestTxId = 0;
        long highestReplicatedTxId = 0;

        Object stblock = new Object();
        List<Node> nodes;
        boolean failureStart = false;

        public void clear() {
            index.clear();
            txIdToWalIndex.clear();
            wal.clear();
            highwaters.clear();
            added = 0;
            walEntryStreamed = 0;
            walHighwaterStreamed = 0;
            highestReplicatedTxId = 0;
        }

        public Node(Random rand,
            OrderIdProvider txIdProvider,
            String name,
            long version,
            long start,
            long step,
            long stop,
            long pauseBetweenAdds,
            long stbAfterNAdds,
            List<Node> nodes) {

            this.rand = rand;
            this.txIdProvider = txIdProvider;
            this.name = name;
            this.version = version;
            this.start = start;
            this.step = step;
            this.stop = stop;
            this.pauseBetweenAdds = pauseBetweenAdds;
            this.stbAfterNAdds = stbAfterNAdds;
            this.nodes = nodes;
        }

        Set<Long> walToMissingSet(long max) {
            Set<Long> walSet = new TreeSet<>();
            for (long i = 0; i < max; i++) {
                walSet.add(i);
            }
            synchronized (wal) {
                for (Object object : wal) {
                    if (object instanceof WALEntry) {
                        walSet.remove(((WALEntry) object).entry);
                    }
                }
            }
            return walSet;
        }

        boolean isValid(long max) {
            Set<Long> set = new HashSet<>();
            synchronized (wal) {
                for (Object object : wal) {
                    if (object instanceof WALEntry) {
                        WALEntry we = (WALEntry) object;
                        if (set.contains(we.entry)) {
                            return false;
                        }
                        set.add(we.entry);
                    }
                }
            }
            System.out.println(name + ":" + version + " " + set.size() + "/" + max
                + " hightestTxId:" + highestTxId
                + " we:" + walEntryStreamed
                + " wh:" + walHighwaterStreamed
                + " wal=" + walToMissingSet(max)
                + " start=" + start
                + " step=" + step
                + " stop=" + stop
            );
            for (Node node : nodes) {
                if (node == this) {
                    continue;
                }
                Long got = highwaters.get(new NodeKey(node.name, node.version));
                if (got == null) {
                    System.out.println(name + " does not have highwaters for " + node.name);
                    return false;
                } else if (got < node.highestTxId) {
                    System.out.println(name + " need to catchup " + got + " < " + node.highestTxId);
                    return false;
                }
            }
            return set.size() == max;
        }

        @Override
        public Void call() throws InterruptedException {
            int stb = 0;
            for (long i = start; i < stop; i += step) {
                actualyAdded.add(i);
                long txId = add(i);
                if (stb == stbAfterNAdds) {
                    while (highestReplicatedTxId < txId) {
                        Thread.sleep(10);
                    }
                    synchronized (stblock) {
                        System.out.println("//////// STB ///////");
                        clear();
                        version++;
                        failureStart = true;
                    }
                }
                stb++;
                Thread.sleep(pauseBetweenAdds);
            }
            return null;
        }

        private long add(long entry) {
            long[] txId = new long[1];
            index.computeIfAbsent(entry, (key) -> {
                synchronized (wal) {
                    txId[0] = txIdProvider.nextId();
                    highestTxId = Math.max(highestTxId, txId[0]);
                    int index = wal.size();
                    wal.add(new WALEntry(txId[0], entry));
                    txIdToWalIndex.put(txId[0], index);
                    added++;
                }
                if (added % 100 == 0) {
                    synchronized (wal) {
                        wal.add(new WALHighwater(highwaters));
                    }
                }
                return key;
            });
            return txId[0];
        }

        public void take() {
            synchronized (stblock) {
                int i = 0;
                for (Node node : nodes) {
                    if (node == this) {
                        break;
                    }
                    i++;
                }

                Node next = nodes.get((i + 1) % nodes.size());
                if (!failureStart) {
                    this.take(next, new TakeCursors(next.highwaters), next);
                }
            }
        }

        private void take(Node node, TakeCursors cursors, TakeStream takeStream) {

            WALHighwater highwatersCopy;
            long highestTxIdCopy;
            int count;
            synchronized (wal) {
                highwatersCopy = new WALHighwater(highwaters);
                highestTxIdCopy = highestTxId;
                count = wal.size();
            }
            Long highwaterTxId = cursors.highwaters.get(new NodeKey(name, version));
            Long firstKey = (txIdToWalIndex.isEmpty()) ? null : txIdToWalIndex.firstKey();
            if (firstKey == null) {
                stream(count, count, highwatersCopy, highestTxIdCopy, takeStream);
            } else if (magicEnabled && (version == 1 && (highwaterTxId == null || highwaterTxId < firstKey))) {
                int index = 0;
                DONE:
                for (int i = 0; i < count; i++) {
                    Object object = wal.get(i);
                    if (object instanceof WALHighwater) {
                        WALHighwater walHighwater = (WALHighwater) object;
                        if (!walHighwater.highwaters.isEmpty()) {
                            int matched = 0;
                            for (NodeKey nodeKey : walHighwater.highwaters.keySet()) {
                                if (nodeKey.equals(new NodeKey(name, version))) {
                                    continue;
                                }
                                if (nodeKey.equals(new NodeKey(node.name, node.version))) {
                                    continue;
                                }
                                matched++;
                                Long local = walHighwater.highwaters.get(nodeKey);
                                Long cursor = cursors.highwaters.get(nodeKey);
                                if (cursor == null || local > cursor) {
                                    break DONE;
                                }
                            }
                            if (matched > 0) {
                                index = i;
                            } else {
                                break DONE;
                            }
                        }
                    }
                }
                System.out.println("Magic " + index);
                stream(index, (int) count, highwatersCopy, highestTxIdCopy, takeStream);
            } else {
                //System.out.println("Normal");
                long index = 0;
                if (highwaterTxId != null) {
                    Long txId = txIdToWalIndex.higherKey(highwaterTxId);
                    if (txId != null) {
                        index = txIdToWalIndex.get(txId);
                    } else {
                        index = count;
                    }
                }
                //System.out.println(node.name + " took from " + index + " to " + count);
                stream(index, count, highwatersCopy, highestTxIdCopy, takeStream);
            }

        }

        private boolean stream(long offset, long length, WALHighwater walhighwaters, long highestTxId, TakeStream takeStream) {
            NodeKey nodeKey = new NodeKey(name, version);
            for (long i = offset; i < length; i++) {
                Object w = wal.get((int) i);
                if (w instanceof WALEntry) {
                    if (!takeStream.stream(nodeKey, (WALEntry) w, null, null)) {
                        return false;
                    }
                } else if (!takeStream.stream(nodeKey, null, (WALHighwater) w, null)) {
                    return false;
                }
            }
            takeStream.stream(nodeKey, null, walhighwaters, highestTxId);
            highestReplicatedTxId = Math.max(highestReplicatedTxId, highestTxId);
            return true;
        }

        @Override
        public boolean stream(NodeKey nodeKey, WALEntry walEntry, WALHighwater walHighwater, Long highestTxId) {

            NodeKey self = new NodeKey(name, version);
            if (walEntry != null) {
                walEntryStreamed++;
                highwaters.compute(nodeKey, (key, currentTxId) -> Math.max(walEntry.txId, (currentTxId == null) ? -1 : currentTxId));
                add(walEntry.entry);
            }
            if (walHighwater != null) {
                walHighwaterStreamed++;
                for (Map.Entry<NodeKey, Long> walObject : walHighwater.highwaters.entrySet()) {
                    if (walObject.getKey().equals(self)) {
                        continue;
                    }
                    highwaters.compute(walObject.getKey(), (key, currentTxId) -> Math.max(walObject.getValue(), (currentTxId == null) ? -1 : currentTxId));
                }
            }
            if (highestTxId != null) {
                highwaters.compute(nodeKey, (key, currentTxId) -> Math.max(highestTxId, (currentTxId == null) ? -1 : currentTxId));
                failureStart = false;
            }
            return true;
        }
    }

    static interface TakeStream {

        boolean stream(NodeKey nodeKey, WALEntry wal, WALHighwater highwater, Long highestTxId);
    }

    static class WALEntry {

        long txId;
        long entry;

        public WALEntry(long txId, long entry) {
            this.txId = txId;
            this.entry = entry;
        }
    }

    static class WALHighwater {

        Map<NodeKey, Long> highwaters;

        public WALHighwater(Map<NodeKey, Long> highwaters) {
            this.highwaters = new HashMap<>(highwaters);
        }

    }

    static class TakeCursors {

        Map<NodeKey, Long> highwaters;

        public TakeCursors(Map<NodeKey, Long> highwaters) {
            this.highwaters = new HashMap<>(highwaters);
        }

    }

    static class NodeKey implements Comparable<NodeKey> {

        String name;
        long version;

        public NodeKey(String name, long version) {
            this.name = name;
            this.version = version;
        }

        @Override
        public String toString() {
            return name + ":" + version;
        }

        @Override
        public int compareTo(NodeKey o) {
            int c = name.compareTo(o.name);
            if (c != 0) {
                return c;
            }
            return Long.compare(version, o.version);
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 11 * hash + Objects.hashCode(this.name);
            hash = 11 * hash + (int) (this.version ^ (this.version >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final NodeKey other = (NodeKey) obj;
            if (this.version != other.version) {
                return false;
            }
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            return true;
        }

    }

}
