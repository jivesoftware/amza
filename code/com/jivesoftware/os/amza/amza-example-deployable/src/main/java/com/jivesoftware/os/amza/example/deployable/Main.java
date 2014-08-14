/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.example.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jivesoftware.os.amza.example.deployable.endpoints.AmzaExampleEndpoints;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.Flusher;
import com.jivesoftware.os.amza.shared.MemoryRowsIndex;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.RowsIndexProvider;
import com.jivesoftware.os.amza.shared.RowsStorage;
import com.jivesoftware.os.amza.shared.RowsStorageProvider;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.storage.RowTable;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowReader;
import com.jivesoftware.os.amza.storage.binary.BinaryRowWriter;
import com.jivesoftware.os.amza.storage.filer.Filer;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesSender;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesSender;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesTaker;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpServer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpServerInitializer;
import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.os.server.http.jetty.jersey.server.JerseyEndpoints;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.File;
import java.util.Random;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class Main {

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {

        String hostname = args[0];
        int port = Integer.parseInt(System.getProperty("amza.port", "1175"));
        String multicastGroup = System.getProperty("amza.discovery.group", "225.4.5.6");
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", "1123"));
        String clusterName = (args.length > 1 ? args[1] : null);
        String transport = System.getProperty("amza.transport", "http");

        //TODO pull from properties
        int connectionsPerHost = Integer.parseInt(System.getProperty("amza.tcp.client.connectionsPerHost", "2"));
        int connectTimeoutMillis = Integer.parseInt(System.getProperty("amza.tcp.client.connectTimeoutMillis", "30000"));
        int socketTimeoutMillis = Integer.parseInt(System.getProperty("amza.tcp.client.socketTimeoutMillis", "30000"));
        int bufferSize = Integer.parseInt(System.getProperty("amza.tcp.bufferSize", "" + (1024 * 1024 * 10)));
        int numServerThreads = Integer.parseInt(System.getProperty("amza.tcp.server.numThreads", "4"));
        int numBuffers = numServerThreads * 10;
        int tcpPort = Integer.parseInt(System.getProperty("amza.tcp.port", "1177"));

        RingHost ringHost;
        if (transport.equals("http")) {
            ringHost = new RingHost(hostname, port); // TODO include rackId
        } else {
            ringHost = new RingHost(hostname, tcpPort); // TODO include rackId
        }

        // todo need a better way to create writter id.
        final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(new Random().nextInt(512)));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();

        RowsStorageProvider rowsStorageProvider = new RowsStorageProvider() {
            @Override
            public RowsStorage createRowsStorage(File workingDirectory,
                    String tableDomain,
                    TableName tableName) throws Exception {
                final File directory = new File(workingDirectory, tableDomain);
                directory.mkdirs();
                File file = new File(directory, tableName.getTableName() + ".kvt");

                Filer filer = new Filer(file.getAbsolutePath(), "rw");
                BinaryRowReader reader = new BinaryRowReader(filer);
                BinaryRowWriter writer = new BinaryRowWriter(filer);
                BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();


                RowsIndexProvider tableIndexProvider = new RowsIndexProvider() {

                    @Override
                    public RowsIndex createRowsIndex(TableName tableName) throws Exception {
                        final DB db = DBMaker.newDirectMemoryDB()
                            .closeOnJvmShutdown()
                            .make();
                        BTreeMap<RowIndexKey, RowIndexValue> treeMap = db.getTreeMap(tableName.getTableName());
                        return new MemoryRowsIndex(treeMap, new Flusher() {

                            @Override
                            public void flush() {
                                db.commit();
                            }
                        });
                    }
                };

                return new RowTable(tableName,
                        orderIdProvider,
                        tableIndexProvider,
                        rowMarshaller,
                        reader,
                        writer);
            }
        };

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        IndexReplicationProtocol clientProtocol = new IndexReplicationProtocol(null, orderIdProvider);

        MessageFramer framer = new MessageFramer(marshaller, clientProtocol);
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 5);

        TcpClientProvider tcpClientProvider = new TcpClientProvider(
                connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);

        UpdatesSender changeSetSender = new TcpUpdatesSender(tcpClientProvider, clientProtocol);
        UpdatesTaker tableTaker = new TcpUpdatesTaker(tcpClientProvider, clientProtocol);

        if (transport.equals("http")) {
            changeSetSender = new HttpUpdatesSender();
            tableTaker = new HttpUpdatesTaker();
        }

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
                orderIdProvider,
                new com.jivesoftware.os.amza.storage.FstMarshaller(FSTConfiguration.getDefaultConfiguration()),
                rowsStorageProvider,
                rowsStorageProvider,
                rowsStorageProvider,
                changeSetSender,
                tableTaker,
                new RowChanges() {
                    @Override
                    public void changes(RowsChanged changes) throws Exception {
                    }
                });

        amzaService.start(ringHost, amzaServiceConfig.resendReplicasIntervalInMillis,
                amzaServiceConfig.applyReplicasIntervalInMillis,
                amzaServiceConfig.takeFromNeighborsIntervalInMillis,
                amzaServiceConfig.compactTombstoneIfOlderThanNMillis);

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        IndexReplicationProtocol serverProtocol = new IndexReplicationProtocol(amzaService, orderIdProvider);
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 5);
        TcpServerInitializer initializer = new TcpServerInitializer();
        // TODO include rackId
        final TcpServer server = initializer.initialize(new RingHost(hostname, tcpPort), numServerThreads, bufferProvider, framer, serverProtocol);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (InterruptedException ex) {
                    System.out.println("Failed to stop Tcp Replication Service");
                    ex.printStackTrace(System.out);
                }
            }
        });

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Tcp Replication Service Online");
        System.out.println("-----------------------------------------------------------------------");

        JerseyEndpoints jerseyEndpoints = new JerseyEndpoints()
                .addEndpoint(AmzaExampleEndpoints.class)
                .addInjectable(AmzaService.class, amzaService)
                .addEndpoint(AmzaReplicationRestEndpoints.class)
                .addInjectable(AmzaInstance.class, amzaService);

        InitializeRestfulServer initializeRestfulServer = new InitializeRestfulServer(port, "AmzaNode", 128, 10000);
        initializeRestfulServer.addContextHandler("/", jerseyEndpoints);
        ServiceHandle serviceHandle = initializeRestfulServer.build();
        serviceHandle.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Jetty Service Online");
        System.out.println("-----------------------------------------------------------------------");

        if (clusterName != null) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService, ringHost, clusterName, multicastGroup, multicastPort);
            amzaDiscovery.start();
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Discovery Online");
            System.out.println("-----------------------------------------------------------------------");
        } else {
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amze Service is in manual Discovery mode.  No cluster name was specified");
            System.out.println("-----------------------------------------------------------------------");
        }
    }
}
