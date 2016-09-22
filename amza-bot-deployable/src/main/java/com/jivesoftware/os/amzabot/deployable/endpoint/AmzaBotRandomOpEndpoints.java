package com.jivesoftware.os.amzabot.deployable.endpoint;

import com.jivesoftware.os.amzabot.deployable.AmzaBotUtil;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotRandomOpService;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/api/amzabot/v1")
public class AmzaBotRandomOpEndpoints {

    private final AmzaBotRandomOpService service;

    public AmzaBotRandomOpEndpoints(@Context AmzaBotRandomOpService service) {
        this.service = service;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/validKeys")
    public Response getValidKeys() {
        StringBuilder sb = new StringBuilder();
        service.getKeyMap().forEach((key, value) -> {
            sb.append(key);
            sb.append(":");
            sb.append(value);
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
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue());
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
