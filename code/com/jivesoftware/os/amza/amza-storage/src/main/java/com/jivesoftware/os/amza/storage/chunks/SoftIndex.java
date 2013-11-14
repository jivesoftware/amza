/*
 * SoftIndex.java.java
 *
 * Created on 03-12-2010 11:24:38 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.storage.chunks;

import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Must call closeIndex to release worker thread
 *
 * @param <V>
 * @param <K>
 * @param <P>
 */
public class SoftIndex<V, K, P> {

    private final ConcurrentHashMap<K, SoftIndexRef<V, K, P>> set = new ConcurrentHashMap<>();
    private final ReferenceQueue softIndexQueue;
    /**
     *
     */
    public Object name = "SoftIndex";

    /**
     *
     * @param _name
     */
    public SoftIndex(Object _name) {
        if (_name != null) {
            name = _name;
        }
        softIndexQueue = new ReferenceQueue();
        cleaner();
    }

    @Override
    public String toString() {
        return name + " (" + set.size() + ")";
    }

    /**
     *
     * @return
     */
    public long getCount() {
        return set.size();
    }

    /**
     *
     * @param _key
     * @return
     */
    public V get(K _key) {
        synchronized (set) {
            SoftIndexRef<V, K, P> ref = set.get(_key);
            if (ref == null) {
                return null;
            }
            V got = ref.get();
            if (got == null) {
                set.remove(ref.getKey());
            }
            return got;
        }
    }

    /**
     *
     * @return
     */
    public List<Object> getAll() {
        synchronized (set) {
            Collection<SoftIndexRef<V, K, P>> all = set.values();
            List<Object> gots = new ArrayList<>(all.size());
            for (SoftIndexRef<V, K, P> a : all) {
                V got = a.get();
                if (got == null) {
                    set.remove(a.getKey());
                } else {
                    gots.add(got);
                }
            }
            return gots;
        }
    }

    /**
     *
     * @param _key
     * @return
     */
    public SoftIndexRef<V, K, P> getRef(K _key) {
        synchronized (set) {
            SoftIndexRef<V, K, P> ref = set.get(_key);
            if (ref == null) {
                return null;
            }
            V got = ref.get();
            if (got == null) {
                set.remove(ref.getKey());
            }
            return ref;
        }
    }

    /**
     *
     * @param _key
     */
    public void remove(K _key) {
        synchronized (set) {
            SoftIndexRef<V, K, P> ref = set.get(_key);
            if (ref != null) {
                set.remove(ref.getKey());
            }
        }
    }

    /**
     *
     * @param _value
     * @param _key
     * @return
     */
    public Object set(V _value, K _key) {
        synchronized (set) {
            return set(_value, _key, null);
        }
    }

    /**
     *
     * @param _value
     * @param _key
     * @param _payload
     * @return
     */
    public V set(V _value, K _key, P _payload) {
        synchronized (set) {
            if (cleanerThreadRunning) {
                SoftIndexRef<V, K, P> ref = set.get(_key);
                if (ref != null) {
                    V got = ref.get();
                    if (got != null) {
                        return got;
                    }
                }
                SoftIndexRef softRef = new SoftIndexRef(_value, _key, _payload,
                        softIndexQueue);
                set.put(_key, softRef);
            } else {
                SoftIndexRef softRef = new SoftIndexRef(_value, _key, _payload,
                        null);

            }
            return _value;
        }
    }
    private boolean clearing = false;

    /**
     *
     */
    public void clear() {
        synchronized (set) {
            clearing = true;
        }
        while (set.size() > 0) {
            set.clear();
        }
        synchronized (set) {
            clearing = false;
        }
    }

    /**
     *
     */
    public void closeIndex() {
        stopCleanerThread = true;
        while (isCleanerThreadRunning()) {
            Thread _cleaner = cleaner;
            if (_cleaner != null) {
                _cleaner.interrupt();
            }
        }
        clear();
    }

    /**
     *
     * @return
     */
    public boolean isIndexValid() {
        synchronized (set) {
            return clearing ? false : cleanerThreadRunning;
        }
    }
    private Thread cleaner;
    private boolean cleanerThreadRunning = false;
    private boolean stopCleanerThread = false;

    private boolean isCleanerThreadRunning() {
        synchronized (set) {
            return cleanerThreadRunning;
        }
    }

    private void cleaner() {
        cleaner = new Thread() {

            @Override
            public void run() {
                do {
                    try {
                        SoftIndexRef ref = null;
                        try {
                            ref = (SoftIndexRef) softIndexQueue.remove(); // Blocks
                        } catch (InterruptedException x) {
                            continue;
                        }
                        synchronized (set) {
                            Object key = ref.getKey();
                            if (key != null) {
                                SoftIndexRef got = set.get(key);
                                if (got != null && got.get() != null) {
                                    continue;
                                }
                                set.remove(key);
                            }
                        }
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                } while (isCleanerThreadRunning() || stopCleanerThread);
                synchronized (set) {
                    cleanerThreadRunning = false;
                    cleaner = null;
                }
            }
        };
        synchronized (set) {
            cleanerThreadRunning = true;
            cleaner.start();
        }
    }
}