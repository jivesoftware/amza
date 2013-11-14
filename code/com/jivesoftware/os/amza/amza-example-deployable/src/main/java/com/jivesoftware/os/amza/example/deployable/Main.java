package com.jivesoftware.os.amza.example.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import com.jivesoftware.os.amza.storage.FileBackedTableStorage;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.RowTableFile;
import com.jivesoftware.os.amza.storage.json.StringRowReader;
import com.jivesoftware.os.amza.storage.json.StringRowValueChunkMarshaller;
import com.jivesoftware.os.amza.storage.json.StringRowWriter;
import com.jivesoftware.os.amza.transport.http.replication.HttpChangeSetSender;
import com.jivesoftware.os.amza.transport.http.replication.HttpChangeSetTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.os.server.http.jetty.jersey.server.JerseyEndpoints;
import java.io.File;

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

        RingHost ringHost = new RingHost(hostname, port);
        final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(ringHost.getPort(), // todo need a better way to create writter id.
                new AmzaChangeIdPacker(),
                new JiveEpochTimestampProvider());

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();

        TableStorageProvider tableStorageProvider = new TableStorageProvider() {
            @Override
            public <K, V> TableStorage<K, V> createTableStorage(File workingDirectory, String tableDomain, TableName<K, V> tableName) throws Exception {
                File directory = new File(workingDirectory, tableDomain);
                directory.mkdirs();
                File file = new File(directory, tableName.getTableName() + ".kvt");
                StringRowReader reader = new StringRowReader(file);
                StringRowWriter writer = new StringRowWriter(file);

                //RowMarshaller<K, V, String> rowMarshaller = new StringRowMarshaller<>(mapper, tableName);
                RowMarshaller<K, V, String> rowMarshaller = new StringRowValueChunkMarshaller(directory, mapper, tableName);
                RowTableFile<K, V, String> rowTableFile = new RowTableFile<>(orderIdProvider, rowMarshaller, reader, writer);
                return new FileBackedTableStorage(rowTableFile);
            }
        };

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
                orderIdProvider,
                tableStorageProvider,
                tableStorageProvider,
                tableStorageProvider,
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