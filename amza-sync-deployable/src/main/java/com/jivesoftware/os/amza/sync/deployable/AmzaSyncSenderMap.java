package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.collection.AmzaMap;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;

/**
 *
 */
public class AmzaSyncSenderMap extends AmzaMap<String, AmzaSyncSenderConfig> implements AmzaSyncSenderConfigProvider {

    public AmzaSyncSenderMap(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaMarshaller<String> keyMarshaller,
        AmzaMarshaller<AmzaSyncSenderConfig> valueMarshaller) {
        super(clientProvider,partitionName,partitionProperties,keyMarshaller,valueMarshaller);
    }

}
