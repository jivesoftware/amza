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

class AmzaBotHealthCheck implements HealthCheck {

    interface AmzaBotHealthCheckConfig extends HealthCheckConfig {
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
    private final AmzaKeyClearingHousePool amzaKeyClearingHousePool;

    AmzaBotHealthCheck(InstanceConfig instanceConfig,
        AmzaBotHealthCheckConfig config,
        AmzaKeyClearingHousePool amzaKeyClearingHousePool) {
        this.instanceConfig = instanceConfig;
        this.config = config;
        this.amzaKeyClearingHousePool = amzaKeyClearingHousePool;
    }

    public HealthCheckResponse checkHealth() throws Exception {
        Map<String, Entry<String, String>> quarantinedKeys = amzaKeyClearingHousePool.getAllQuarantinedEntries();

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
                        sb.append(AmzaBotUtil.truncVal(entry.getKey()));
                        sb.append(":");
                        sb.append(AmzaBotUtil.truncVal(entry.getValue()));
                    });

                    return sb.toString();
                }

                @Override
                public String getResolution() {
                    return "Investigate invalid Amza keys. Reset via " +
                        "http://" +
                        instanceConfig.getHost() + ":" + instanceConfig.getMainPort() +
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
