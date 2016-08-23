package com.jivesoftware.os.amzabot.deployable.endpoint;

import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.eclipse.jetty.http.HttpParser.LOG;

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
            service.delete(key);
            return Response.accepted().build();
        } catch (Exception e) {
            LOG.error("Failed to handle delete name:{}", new Object[] { key }, e);
            return Response.serverError().build();
        }
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/validKeys")
    public Response getValidKeys() {
        StringBuilder sb = new StringBuilder();
        service.getKeyMap().forEach((key, value) -> {
            sb.append(key);
            sb.append(":");
            sb.append(AmzaBotService.truncVal(value));
            sb.append("\n");
        });

        return Response.ok(sb.toString(), MediaType.TEXT_PLAIN).build();
    }

    @POST
    @Path("/resetValidKeys")
    public Response resetKeys() {
        service.clearKeyMap();
        return Response.accepted().build();
    }

    @GET
    @Path("/invalidKeys")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getInvalidKeys() {
        StringBuilder sb = new StringBuilder();
        service.getQuarantinedKeyMap().forEach((key, entry) -> {
            sb.append(key);
            sb.append(":");
            sb.append(AmzaBotService.truncVal(entry.getKey()));
            sb.append(":");
            sb.append(AmzaBotService.truncVal(entry.getValue()));
            sb.append("\n");
        });

        return Response.ok(sb.toString(), MediaType.TEXT_PLAIN).build();
    }

    @POST
    @Path("/resetInvalidKeys")
    public Response resetErrors() {
        service.clearQuarantinedKeyMap();
        return Response.accepted().build();
    }

}
