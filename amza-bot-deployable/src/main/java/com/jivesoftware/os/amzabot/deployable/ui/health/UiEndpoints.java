package com.jivesoftware.os.amzabot.deployable.ui.health;

import com.google.inject.Singleton;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/")
public class UiEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final UiService uiService;

    public UiEndpoints(@Context UiService uiService) {
        this.uiService = uiService;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        try {
            String rendered = uiService.render();
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Error while rendering", t);
            throw t;
        }
    }

}
