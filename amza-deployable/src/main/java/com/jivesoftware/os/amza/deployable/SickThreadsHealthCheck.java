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

import com.jivesoftware.os.amza.service.SickThreads;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import java.util.Map;

/**
 *
 * @author jonathan.colt
 */
public class SickThreadsHealthCheck implements HealthCheck {

    private final SickThreads sickThreads;

    public SickThreadsHealthCheck(SickThreads sickThreads) {
        this.sickThreads = sickThreads;
    }

    @Override
    public HealthCheckResponse checkHealth() throws Exception {
        Map<Thread, Throwable> sickThread = sickThreads.getSickThread();
        if (sickThread.isEmpty()) {
            return new HealthCheckResponseImpl("sick>threads", 1.0, "Healthy", "No sick threads", "", System.currentTimeMillis());
        } else {
            return new HealthCheckResponse() {

                @Override
                public String getName() {
                    return "sick>thread";
                }

                @Override
                public double getHealth() {
                    return 0;
                }

                @Override
                public String getStatus() {
                    return "There are " + sickThread.size() + " sick threads.";
                }

                @Override
                public String getDescription() {
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<Thread, Throwable> entry : sickThread.entrySet()) {
                        sb.append("thread:").append(entry.getKey()).append(" cause:").append(entry.getValue());
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
