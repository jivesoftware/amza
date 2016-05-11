package com.jivesoftware.os.amza.service.take;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jersey.repackaged.com.google.common.collect.Maps;

/**
 *
 * @author jonathan.colt
 */
public class Interruptables implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final long interruptBlockingReadsIfLingersForNMillis;
    private final ScheduledExecutorService hanguper;
    private final Map<Thread, Interruptable> hangupables = Maps.newConcurrentMap();

    public Interruptables(String name, long interruptBlockingReadsIfLingersForNMillis) {
        this.interruptBlockingReadsIfLingersForNMillis = interruptBlockingReadsIfLingersForNMillis;
        this.hanguper = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(name + "-hanguper-%d").build());

    }

    public static class Interruptable {

        private final Thread thread;
        private final long interruptBlockingReadsIfLingersForNMillis;
        private final Map<Thread, Interruptable> interruptables;

        private volatile long touchTimestamp = System.currentTimeMillis();

        Interruptable(Thread thread, long interruptBlockingReadsIfLingersForNMillis, Map<Thread, Interruptable> hangupables) {
            this.thread = thread;
            this.interruptBlockingReadsIfLingersForNMillis = interruptBlockingReadsIfLingersForNMillis;
            this.interruptables = hangupables;
        }

        public void alive() {
            touchTimestamp = System.currentTimeMillis();
        }

        public boolean hangupIfNecessary() {
            if (touchTimestamp + interruptBlockingReadsIfLingersForNMillis < System.currentTimeMillis()) {
                LOG.info("Hanging up connection for thread:{}. This should typically happen when you loose a server or a GC pause exceeds: {} ", thread,
                    interruptBlockingReadsIfLingersForNMillis);
                interruptables.remove(thread);
                thread.interrupt();
                return true;
            }
            return false;
        }
    }

    public Interruptable aquire() {
        return hangupables.computeIfAbsent(Thread.currentThread(), (t) -> new Interruptable(t, interruptBlockingReadsIfLingersForNMillis, hangupables));
    }

    public void release() {
        hangupables.remove(Thread.currentThread());
    }

    @Override
    public void run() {
        for (Interruptable hangupable : hangupables.values()) {
            try {
                hangupable.hangupIfNecessary();
            } catch (Exception x) {
                LOG.warn("Error while hanging up connection.", x);
            }
        }
    }

    public void start() {
        hanguper.scheduleWithFixedDelay(this, interruptBlockingReadsIfLingersForNMillis, interruptBlockingReadsIfLingersForNMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        hanguper.shutdownNow();
    }
}
