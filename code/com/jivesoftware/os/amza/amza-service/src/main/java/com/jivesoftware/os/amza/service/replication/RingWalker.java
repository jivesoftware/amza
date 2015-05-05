/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.RingHost;

class RingWalker {

    private final RingHost[] ringHosts;
    private final int replicationFactor;
    int replicated = 0;
    int loops = 0;
    int failed = 0;

    public RingWalker(RingHost[] ringHosts, int replicationFactor) {
        this.ringHosts = ringHosts;
        this.replicationFactor = replicationFactor;
    }

    public RingHost host() {
        int i = failed + (loops * 2);
        if (i >= ringHosts.length) {
            return null;
        }
        if (replicated >= replicationFactor) {
            return null;
        }
        RingHost ringHost = ringHosts[i];
        ringHosts[i] = null;
        return ringHost;
    }

    public void success() {
        loops++;
        failed = 0;
        replicated++;
    }

    public void failed() {
        failed++;
    }

    public int getNumReplicated() {
        return replicated;
    }

}
