package com.jivesoftware.os.amzabot.deployable.endpoint;

import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import io.swagger.annotations.Api;
import java.util.Map.Entry;
import java.util.Set;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(value = "Amza Bot")
@Singleton
@Path("/api/amzabot/v1")
public class AmzaBotEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotService service;

    public AmzaBotEndpoints(@Context AmzaBotService service) {
        this.service = service;
    }

    @POST
    @Path("/keys/{name}")
    public Response set(@PathParam("name") String key, String value) {
        try {
            service.set(key, value);
            return Response.accepted().build();
        } catch (Exception e) {
            LOG.error("Failed to handle set name:{} {}", new Object[] { key, value }, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/keys")
    public Response set(Set<Entry<String, String>> entries) {
        try {
            service.multiSet(entries);
            return Response.accepted().build();
        } catch (Exception e) {
            LOG.error("Failed to handle set batch: {}", new Object[] { entries }, e);
            return Response.serverError().build();
        }
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/keys/{name}")
    public Response get(@PathParam("name") String key) {
        try {
            String value = service.get(key);
            if (value == null) {
                return Response.noContent().build();
            }

            return Response.ok(value).build();
        } catch (Exception e) {
            LOG.error("Failed to handle get name:{}", new Object[] { key }, e);
            return Response.serverError().build();
        }
    }

    @DELETE
    @Path("/keys/{name}")
    public Response delete(@PathParam("name") String key) {
        try {
            String value = service.delete(key);
            if (value == null) {
                return Response.noContent().build();
            }

            return Response.accepted().build();
        } catch (Exception e) {
            LOG.error("Failed to handle delete name:{}", new Object[] { key }, e);
            return Response.serverError().build();
        }
    }

}
