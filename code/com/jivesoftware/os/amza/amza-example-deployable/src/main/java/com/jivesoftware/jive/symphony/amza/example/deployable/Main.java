package com.jivesoftware.jive.symphony.amza.example.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jivesoftware.os.amza.transport.http.replication.HttpChangeSetSender;
import com.jivesoftware.os.amza.transport.http.replication.HttpChangeSetTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.example.deployable.endpoints.AmzaExampleEndpoints;
import com.jivesoftware.os.amza.service.AmzaChangeIdPacker;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.RowMarshallerProvider;
import com.jivesoftware.os.amza.storage.json.StringRowMarshaller;
import com.jivesoftware.os.amza.storage.json.StringTableStorageProvider;
import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.os.server.http.jetty.jersey.server.JerseyEndpoints;

public class Main {

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {

        String clusterName = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        RingHost ringHost = new RingHost(host, port);
        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(ringHost.getPort(), // todo need a better way to create writter id.
                new AmzaChangeIdPacker(),
                new JiveEpochTimestampProvider());

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        StringTableStorageProvider jsonPartitionStorageProvider = new StringTableStorageProvider(orderIdProvider, new RowMarshallerProvider() {

            @Override
            public <K, V, String> RowMarshaller<K, V, String> getRowMarshaller(TableName<K, V> tableName) {
                return (RowMarshaller<K, V, String>) new StringRowMarshaller<>(mapper, tableName);
            }
        });

        AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
                orderIdProvider,
                jsonPartitionStorageProvider,
                jsonPartitionStorageProvider,
                jsonPartitionStorageProvider,
                new HttpChangeSetSender(),
                new HttpChangeSetTaker(),
                new TableStateChanges<Object, Object>() {

                    @Override
                    public void changes(TableName<Object, Object> tableName, TableDelta<Object, Object> changes) throws Exception {
                    }
                });

        amzaService.start(ringHost, amzaServiceConfig.resendReplicasIntervalInMillis,
                amzaServiceConfig.applyReplicasIntervalInMillis,
                amzaServiceConfig.takeFromNeighborsIntervalInMillis,
                amzaServiceConfig.compactTombstoneIfOlderThanNMillis);

//        try {
//            amzaService.addRingHost("master", ringHost);
//        } catch (Exception x) {
//            x.printStackTrace();
//            System.exit(1);
//        }


//        for (String peer : peers) {
//            try {
//                hostPort = peer.split(":");
//                amzaService.addRingHost("master", new RingHost(hostPort[0], Integer.parseInt(hostPort[1])));
//            } catch (Exception x) {
//                System.out.println("Failed to add ring host:" + peer + " Cause:" + x.getMessage());
//            }
//        }

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
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

        AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService, ringHost, clusterName, "225.4.5.6", 9876);
        amzaDiscovery.start();
    }
}