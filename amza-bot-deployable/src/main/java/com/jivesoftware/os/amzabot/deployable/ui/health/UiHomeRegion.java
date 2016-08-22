package com.jivesoftware.os.amzabot.deployable.ui.health;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amzabot.deployable.ui.SoyRenderer;
import com.jivesoftware.os.amzabot.deployable.ui.UiPageRegion;
import com.jivesoftware.os.mlogger.core.LoggerSummary;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UiHomeRegion implements UiPageRegion<Void> {

    private final String template;
    private final SoyRenderer renderer;

    public UiHomeRegion(String template, SoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        data.put("errors", String.valueOf(LoggerSummary.INSTANCE.errors + LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.errors));
        data.put("recentErrors", recentLogs(LoggerSummary.INSTANCE.lastNErrors.get(), LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.lastNErrors.get()));

        data.put("warns", String.valueOf(LoggerSummary.INSTANCE.warns + LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.warns));
        data.put("recentWarns", recentLogs(LoggerSummary.INSTANCE.lastNWarns.get(), LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.lastNWarns.get()));

        data.put("infos", String.valueOf(LoggerSummary.INSTANCE.infos + LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.infos));
        data.put("recentInfos", recentLogs(LoggerSummary.INSTANCE.lastNInfos.get(), LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.lastNInfos.get()));

        ingressed(data);
        egressed(data);

        return renderer.render(template, data);
    }

    private List<String> recentLogs(String[] internal, String[] external) {
        List<String> log = new ArrayList<>();
        if (internal != null) {
            for (String i : internal) {
                if (i != null) {
                    log.add(i);
                }
            }
        }
        if (external != null) {
            for (String e : external) {
                if (e != null) {
                    log.add(e);
                }
            }
        }
        return log;
    }

    private void ingressed(Map<String, Object> data) {
        data.put("ingressedRecency", "0");
        data.put("ingressedLatency", "0");
        data.put("ingressedTotal", "0");
        data.put("ingressedStatus", Collections.emptyList());
    }

    private void egressed(Map<String, Object> data) {
        data.put("egressedRecency", "0");
        data.put("egressedLatency", "0");
        data.put("egressedTotal", "0");
        data.put("egressedStatus", Collections.emptyList());
    }

    @Override
    public String getTitle(Void input) {
        return "Home - UI";
    }

}
