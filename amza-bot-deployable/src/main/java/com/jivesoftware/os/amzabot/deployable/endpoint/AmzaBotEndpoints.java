package com.jivesoftware.os.amzabot.deployable.endpoint;

import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
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

@Singleton
@Path("/api/amzabot/v1")
public class AmzaBotEndpoints {

    private final AmzaBotService service;

    public AmzaBotEndpoints(@Context AmzaBotService service) {
        this.service = service;
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/keys/{name}")
    public Response get(@PathParam("name") String key) {
        try {
            return Response.ok(service.get(key)).build();
        } catch (Exception e) {
            return Response.serverError().build();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/keys/{name}")
    public Response set(@PathParam("name") String key, String value) {
        try {
            service.set(key, value);
            return Response.accepted().build();
        } catch (Exception e) {
            return Response.serverError().build();
        }
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/keys/{name}")
    public Response delete(@PathParam("name") String key) {
        try {
            service.delete(key);
            return Response.accepted().build();
        } catch (Exception e) {
            return Response.serverError().build();
        }
    }

}
