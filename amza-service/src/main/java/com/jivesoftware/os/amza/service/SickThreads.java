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
package com.jivesoftware.os.amza.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class SickThreads {

    private final Map<Thread, Throwable> sickThread = new ConcurrentHashMap<>();

    public void sick(Throwable throwable) {
        sickThread.put(Thread.currentThread(), throwable);
    }

    public void recovered() {
        sickThread.remove(Thread.currentThread());
    }

    public Map<Thread, Throwable> getSickThread() {
        return sickThread;
    }

}
