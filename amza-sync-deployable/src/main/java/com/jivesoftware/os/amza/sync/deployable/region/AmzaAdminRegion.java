package com.jivesoftware.os.amza.sync.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSender;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenders;
import com.jivesoftware.os.amza.ui.region.PageRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AmzaAdminRegion implements PageRegion<Void> {

    private final String template;
    private final SoyRenderer renderer;
    private final boolean senderEnabled;
    private final boolean receiverEnabled;
    private final AmzaSyncSenders syncSenders;

    public AmzaAdminRegion(String template,
        SoyRenderer renderer,
        boolean senderEnabled,
        boolean receiverEnabled,
        AmzaSyncSenders syncSenders) {
        this.template = template;
        this.renderer = renderer;
        this.senderEnabled = senderEnabled;
        this.receiverEnabled = receiverEnabled;
        this.syncSenders = syncSenders;
    }

    @Override
    public String render(Void input) {
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("sender", senderEnabled);
        data.put("receiver", receiverEnabled);
        if (senderEnabled) {
            Collection<AmzaSyncSender> senders = syncSenders.getActiveSenders();
            List<Map<String, Object>> senderData = Lists.newArrayList();
            for (AmzaSyncSender sender : senders) {
                AmzaSyncSenderConfig config = sender.getConfig();
                senderData.add(ImmutableMap.<String, Object>builder()
                    .put("name", config.name)
                    .put("enabled", config.enabled)
                    .put("senderScheme", config.senderScheme)
                    .put("senderHost", config.senderHost)
                    .put("senderPort", config.senderPort)
                    .put("allowSelfSignedCerts", config.allowSelfSignedCerts)
                    .put("syncIntervalMillis", String.valueOf(config.syncIntervalMillis))
                    .put("batchSize", config.batchSize)
                    .build());
            }


            data.put("senders", senderData);

        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Sync";
    }
}
