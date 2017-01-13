package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionTuple;
import java.util.Map;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public interface AmzaSyncPartitionConfigProvider {

     Map<AmzaSyncPartitionTuple, AmzaSyncPartitionConfig> getAll(String senderName) throws Exception;
}
