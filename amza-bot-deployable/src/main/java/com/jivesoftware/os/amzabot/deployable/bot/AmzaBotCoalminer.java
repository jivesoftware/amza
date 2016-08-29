package com.jivesoftware.os.amzabot.deployable.bot;

import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.amzabot.deployable.AmzaBotUtil;
import com.jivesoftware.os.amzabot.deployable.AmzaKeyClearingHouse;
import com.jivesoftware.os.amzabot.deployable.AmzaKeyClearingHousePool;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class AmzaBotCoalminer implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Random RANDOM = new Random();

    private final AmzaBotCoalmineConfig config;
    private final AmzaBotService service;
    private final AmzaKeyClearingHousePool amzaKeyClearingHousePool;

    public AmzaBotCoalminer(AmzaBotCoalmineConfig config,
        AmzaBotService service,
        AmzaKeyClearingHousePool amzaKeyClearingHousePool) {
        this.config = config;
        this.service = service;
        this.amzaKeyClearingHousePool = amzaKeyClearingHousePool;
    }

    public void run() {
        try {
            LOG.info("Generating a clearing house of {} canaries.", config.getCoalmineCapacity());
            AmzaKeyClearingHouse amzaKeyClearingHouse =
                amzaKeyClearingHousePool.genAmzaKeyClearingHouse(config.getCoalmineCapacity());

            LOG.info("Fill clearing house and partition.");
            {
                AtomicCounter seq = new AtomicCounter();

                Entry<String, String> canary = amzaKeyClearingHouse.genRandomEntry(
                    String.valueOf(seq.getValue()),
                    config.getCanarySizeThreshold());

                while (canary != null) {
                    amzaKeyClearingHouse.set(canary.getKey(), canary.getValue());
                    service.set(canary.getKey(), canary.getValue());

                    LOG.debug("Mined canary {}:{}", canary.getKey(), AmzaBotUtil.truncVal(canary.getValue()));

                    if (config.getHesitationMs() > 0) {
                        Thread.sleep(RANDOM.nextInt(config.getHesitationMs()));
                    }

                    seq.inc();
                    canary = amzaKeyClearingHouse.genRandomEntry(
                        String.valueOf(seq.getValue()),
                        config.getCanarySizeThreshold());
                }
            }

            LOG.info("Verify clearing house matches partition");
            {
                Map<String, String> keyMapCopy = new HashMap<>(amzaKeyClearingHouse.getKeyMap());
                for (Entry<String, String> canary : service.getAll().entrySet()) {
                    String value = keyMapCopy.remove(canary.getKey());

                    if (value == null) {
                        amzaKeyClearingHouse.quarantineEntry(canary, null);
                        LOG.error("Canary not found {}:{}",
                            canary.getKey(),
                            AmzaBotUtil.truncVal(canary.getValue()));
                    } else if (!value.equals(canary.getValue())) {
                        amzaKeyClearingHouse.quarantineEntry(canary, value);
                        LOG.error("Canary value differs {}:{}:{}",
                            canary.getKey(),
                            AmzaBotUtil.truncVal(canary.getValue()),
                            AmzaBotUtil.truncVal(value));
                    }
                }

                for (Entry<String, String> canary : keyMapCopy.entrySet()) {
                    amzaKeyClearingHouse.quarantineEntry(canary, "extra");
                    LOG.error("Extra canary found {}:{}",
                        canary.getKey(),
                        AmzaBotUtil.truncVal(canary.getValue()));
                }
            }

            LOG.info("Drain clearing house and corresponding partition entries");
            {
                Entry<String, String> canary = amzaKeyClearingHouse.popRandomEntry();
                while (canary != null) {
                    String value = service.delete(canary.getKey());

                    if (value == null) {
                        amzaKeyClearingHouse.quarantineEntry(canary, null);
                        LOG.error("Canary not found {}", canary.getKey());
                    } else if (!value.equals(canary.getValue())) {
                        amzaKeyClearingHouse.quarantineEntry(canary, value);
                        LOG.error("Canary value differs {}:{}:{}",
                            canary.getKey(),
                            AmzaBotUtil.truncVal(canary.getValue()),
                            AmzaBotUtil.truncVal(value));
                    }

                    if (config.getHesitationMs() > 0) {
                        Thread.sleep(RANDOM.nextInt(config.getHesitationMs()));
                    }

                    canary = amzaKeyClearingHouse.popRandomEntry();
                }
            }

            LOG.info("Verify the partition is empty");
            {
                for (Entry<String, String> canary : service.getAll().entrySet()) {
                    amzaKeyClearingHouse.quarantineEntry(canary, "extra");
                    LOG.error("Extra canary found {}:{}",
                        canary.getKey(),
                        AmzaBotUtil.truncVal(canary.getValue()));
                }
            }

            if (amzaKeyClearingHouse.getQuarantinedKeyMap().size() == 0) {
                LOG.info("No quarantined keys generated. Removing clearing house.");
                amzaKeyClearingHousePool.removeAmzaKeyClearingHouse(amzaKeyClearingHouse);
            }

            LOG.info("Coalmine test completed");
        } catch (Exception e) {
            LOG.error("Error occurred mining coal", e);
        }
    }

}
