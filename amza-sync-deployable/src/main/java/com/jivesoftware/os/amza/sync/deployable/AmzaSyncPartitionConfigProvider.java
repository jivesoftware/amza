package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import java.util.Map;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public interface AmzaSyncPartitionConfigProvider {

     Map<PartitionName, AmzaSyncPartitionConfig> getAll(String senderName) throws Exception;
}
