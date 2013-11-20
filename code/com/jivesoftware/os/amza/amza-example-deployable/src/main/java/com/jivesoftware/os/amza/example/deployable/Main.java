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
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import com.jivesoftware.os.amza.storage.FileBackedTableStorage;
import com.jivesoftware.os.amza.storage.RowTableFile;
import com.jivesoftware.os.amza.storage.binary.BinaryRowChunkMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowReader;
import com.jivesoftware.os.amza.storage.binary.BinaryRowWriter;
import com.jivesoftware.os.amza.storage.chunks.Filer;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpChangeSetSender;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpChangeSetTaker;
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
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.os.server.http.jetty.jersey.server.JerseyEndpoints;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.File;
import java.util.Random;

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
        final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new Random().nextInt(1024)); // todo need a better way to create writter id.

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

                Filer filer = Filer.open(file, "rw");
                BinaryRowReader reader = new BinaryRowReader(filer);
                BinaryRowWriter writer = new BinaryRowWriter(filer);
                BinaryRowChunkMarshaller rowMarshaller = new BinaryRowChunkMarshaller(directory, tableName);
                RowTableFile<K, V, byte[]> rowTableFile = new RowTableFile<>(orderIdProvider, rowMarshaller, reader, writer);


                /*
                 StringRowReader reader = new StringRowReader(file);
                 StringRowWriter writer = new StringRowWriter(file);

                 //RowMarshaller<K, V, String> rowMarshaller = new StringRowMarshaller<>(mapper, tableName);
                 RowMarshaller<K, V, String> rowMarshaller = new StringRowValueChunkMarshaller(directory, mapper, tableName);
                 RowTableFile<K, V, String> rowTableFile = new RowTableFile<>(orderIdProvider, rowMarshaller, reader, writer);
                 */
                return new FileBackedTableStorage(rowTableFile);
            }
        };

        //TODO pull from properties
        int connectionsPerHost = Integer.parseInt(System.getProperty("amza.tcp.client.connectionsPerHost", "2"));
        int connectTimeoutMillis = Integer.parseInt(System.getProperty("amza.tcp.client.connectTimeoutMillis", "5000"));
        int socketTimeoutMillis = Integer.parseInt(System.getProperty("amza.tcp.client.socketTimeoutMillis", "2000"));
        int bufferSize = Integer.parseInt(System.getProperty("amza.tcp.bufferSize", "2048"));
        int numServerThreads = Integer.parseInt(System.getProperty("amza.tcp.server.numThreads", "4"));
        int numBuffers = numServerThreads + 2;
        int tcpPort = Integer.parseInt(System.getProperty("amza.tcp.port", "1177"));

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        IndexReplicationProtocol clientProtocol = new IndexReplicationProtocol(null, orderIdProvider);

        MessageFramer framer = new MessageFramer(marshaller, clientProtocol);
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true);

        TcpClientProvider tcpClientProvider = new TcpClientProvider(
            connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
            orderIdProvider,
            tableStorageProvider,
            tableStorageProvider,
            tableStorageProvider,
            new TcpChangeSetSender(tcpClientProvider, clientProtocol),
            new TcpChangeSetTaker(tcpClientProvider, clientProtocol),
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


        IndexReplicationProtocol serverProtocol = new IndexReplicationProtocol(amzaService, orderIdProvider);
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true);
        TcpServerInitializer initializer = new TcpServerInitializer();
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
