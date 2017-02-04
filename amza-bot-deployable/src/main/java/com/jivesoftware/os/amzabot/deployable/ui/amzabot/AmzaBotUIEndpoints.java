package com.jivesoftware.os.amzabot.deployable.ui.amzabot;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/amzabot/ui")
public class AmzaBotUIEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotUIService service;

    public AmzaBotUIEndpoints(@Context AmzaBotUIService service) {
        this.service = service;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response getUI(@QueryParam("key") @DefaultValue("") String key) {
        try {
            String rendered = service.render(new AmzaBotInput(key));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to build ui.", x);
            return Response.serverError().build();
        }
    }

}
