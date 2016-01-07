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
package com.jivesoftware.os.amza.deployable;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class SickPartitionsHealthCheck implements HealthCheck {

    private final SickPartitions sickPartitions;

    public SickPartitionsHealthCheck(SickPartitions sickPartitions) {
        this.sickPartitions = sickPartitions;
    }

    @Override
    public HealthCheckResponse checkHealth() throws Exception {
        Map<VersionedPartitionName, Throwable> sickPartitions = this.sickPartitions.getSickPartitions();
        if (sickPartitions.isEmpty()) {
            return new HealthCheckResponseImpl("sick>partition", 1.0, "Healthy", "No sick partitions", "", System.currentTimeMillis());
        } else {
            return new HealthCheckResponse() {

                @Override
                public String getName() {
                    return "sick>partition";
                }

                @Override
                public double getHealth() {
                    return 0;
                }

                @Override
                public String getStatus() {
                    return "There are " + sickPartitions.size() + " sick partitions.";
                }

                @Override
                public String getDescription() {
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<VersionedPartitionName, Throwable> entry : sickPartitions.entrySet()) {
                        sb.append("partition:").append(entry.getKey()).append(" cause:").append(entry.getValue());
                    }
                    return sb.toString();
                }

                @Override
                public String getResolution() {
                    return "Look at the logs and see if you can resolve the issue.";
                }

                @Override
                public long getTimestamp() {
                    return System.currentTimeMillis();
                }
            };
        }
    }

}
