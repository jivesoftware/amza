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
package com.jivesoftware.os.amza.shared.ring;

import java.util.Map.Entry;

public class RingNeighbors {

    private final Entry<RingMember, RingHost>[] aboveRing;
    private final Entry<RingMember, RingHost>[] belowRing;

    public RingNeighbors(Entry<RingMember, RingHost>[] aboveRing, Entry<RingMember, RingHost>[] belowRing) {
        this.aboveRing = aboveRing;
        this.belowRing = belowRing;
    }

    public Entry<RingMember, RingHost>[] getAboveRing() {
        return aboveRing;
    }

    public Entry<RingMember, RingHost>[] getBelowRing() {
        return belowRing;
    }
}
