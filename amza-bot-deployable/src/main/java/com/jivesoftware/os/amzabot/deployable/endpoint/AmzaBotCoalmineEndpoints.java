package com.jivesoftware.os.amzabot.deployable.endpoint;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/api/amzabot/v1")
public class AmzaBotCoalmineEndpoints {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/placeholder")
    public Response getPlaceholder() {
        return Response.ok("placeholder", MediaType.TEXT_PLAIN).build();
    }

}
