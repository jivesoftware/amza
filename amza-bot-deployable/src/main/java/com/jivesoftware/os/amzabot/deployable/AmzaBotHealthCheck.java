/*
 * Copyright 2015 jonathan.colt.
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
package com.jivesoftware.os.amzabot.deployable;

import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckConfig;
import java.util.Map;
import java.util.Map.Entry;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

public class AmzaBotHealthCheck implements HealthCheck {

    public interface AmzaBotHealthCheckConfig extends HealthCheckConfig {
        @Override
        @StringDefault("amzabot>fault>count")
        String getName();

        @Override
        @StringDefault("Number of amza bot faults.")
        String getDescription();

        @DoubleDefault(0.0)
        Double getBadHealth();
    }

    private final InstanceConfig instanceConfig;
    private final AmzaBotHealthCheckConfig config;
    private final AmzaKeyClearingHouse amzaKeyClearingHouse;

    public AmzaBotHealthCheck(InstanceConfig instanceConfig,
        AmzaBotHealthCheckConfig config,
        AmzaKeyClearingHouse amzaKeyClearingHouse) {
        this.instanceConfig = instanceConfig;
        this.config = config;
        this.amzaKeyClearingHouse = amzaKeyClearingHouse;
    }

    public HealthCheckResponse checkHealth() throws Exception {
        Map<String, Entry<String, String>> quarantinedKeys = amzaKeyClearingHouse.getQuarantinedKeyMap();

        if (quarantinedKeys.isEmpty()) {
            return new HealthCheckResponseImpl(config.getName(), 1.0, "Healthy", config.getDescription(), "", System.currentTimeMillis());
        } else {
            return new HealthCheckResponse() {
                @Override
                public String getName() {
                    return config.getName();
                }

                @Override
                public double getHealth() {
                    return config.getBadHealth();
                }

                @Override
                public String getStatus() {
                    return "There are " + quarantinedKeys.size() + " invalid keys.";
                }

                @Override
                public String getDescription() {
                    StringBuilder sb = new StringBuilder("Invalid key list: ");

                    quarantinedKeys.forEach((key, entry) -> {
                        if (sb.length() > 0) {
                            sb.append(",");
                        }
                        sb.append(key);
                        sb.append(":");
                        sb.append(AmzaBotService.truncVal(entry.getKey()));
                        sb.append(":");
                        sb.append(AmzaBotService.truncVal(entry.getValue()));
                    });

                    return sb.toString();
                }

                @Override
                public String getResolution() {
                    return "Investigate invalid Amza keys. Reset via " +
                        "http://" +
                        instanceConfig.getHost() + ":" + instanceConfig.getManagePort() +
                        "/api/amzabot/v1/resetInvalidKeys";
                }

                @Override
                public long getTimestamp() {
                    return System.currentTimeMillis();
                }
            };
        }
    }

}
