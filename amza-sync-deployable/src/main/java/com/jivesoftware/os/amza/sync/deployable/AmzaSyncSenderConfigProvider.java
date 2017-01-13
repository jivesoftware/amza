package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import java.util.Map;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public interface AmzaSyncSenderConfigProvider {

     Map<String, AmzaSyncSenderConfig> getAll() throws Exception;
}
