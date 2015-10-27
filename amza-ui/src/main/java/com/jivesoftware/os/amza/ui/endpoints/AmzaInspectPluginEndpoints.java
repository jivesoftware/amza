package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.ui.region.AmzaInspectPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaInspectPluginRegion.AmzaInspectPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/amza/ui/inspect")
public class AmzaInspectPluginEndpoints {

    private final SoyService soyService;
    private final AmzaInspectPluginRegion pluginRegion;

    public AmzaInspectPluginEndpoints(@Context SoyService soyService, @Context AmzaInspectPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring(@QueryParam("regionType") @DefaultValue("USER") String type,
        @QueryParam("client") @DefaultValue("false") boolean client,
        @QueryParam("ringName") @DefaultValue("") String ringName,
        @QueryParam("regionName") @DefaultValue("") String regionName,
        @QueryParam("prefix") @DefaultValue("") String prefix,
        @QueryParam("key") @DefaultValue("") String key,
        @QueryParam("toPrefix") @DefaultValue("") String toPrefix,
        @QueryParam("toKey") @DefaultValue("") String toKey,
        @QueryParam("value") @DefaultValue("") String value,
        @QueryParam("offset") @DefaultValue("0") int offset,
        @QueryParam("batchSize") @DefaultValue("100") int batchSize,
        @QueryParam("consistency") @DefaultValue("none") String consistency) {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AmzaInspectPluginRegionInput(client, type.equals("SYSTEM"), ringName, regionName, prefix, key, toPrefix, toKey, value, offset, batchSize,
                Consistency.valueOf(consistency), ""));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("client") @DefaultValue("false") boolean client,
        @FormParam("systemRegion") @DefaultValue("false") boolean systemRegion,
        @FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("regionName") @DefaultValue("") String regionName,
        @FormParam("prefix") @DefaultValue("") String prefix,
        @FormParam("key") @DefaultValue("") String key,
        @FormParam("toPrefix") @DefaultValue("") String toPrefix,
        @FormParam("toKey") @DefaultValue("") String toKey,
        @FormParam("value") @DefaultValue("") String value,
        @FormParam("offset") @DefaultValue("0") int offset,
        @FormParam("batchSize") @DefaultValue("100") int batchSize,
        @FormParam("consistency") @DefaultValue("none") String consistency,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AmzaInspectPluginRegionInput(client, systemRegion, ringName, regionName, prefix, key, toPrefix, toKey, value, offset, batchSize,
                Consistency.valueOf(consistency), action));
        return Response.ok(rendered).build();
    }
}
