package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion.AmzaPartitionsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/amza/ui/partitions")
public class AmzaPartitionsPluginEndpoints {

    private final SoyService soyService;
    private final AmzaPartitionsPluginRegion partitions;

    public AmzaPartitionsPluginEndpoints(@Context SoyService soyService, @Context AmzaPartitionsPluginRegion partitions) {
        this.soyService = soyService;
        this.partitions = partitions;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(partitions, new AmzaPartitionsPluginRegionInput("", "", "", "", "none", true, true, RowType.primary));
        return Response.ok(rendered).build();
    }

    @POST
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("action") @DefaultValue("") String action,
        @FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("indexClassName") @DefaultValue("berkeleydb") String indexClassName,
        @FormParam("name") @DefaultValue("") String partitionName,
        @FormParam("consistency") @DefaultValue("none") String consistency,
        @FormParam("requireConsistency") @DefaultValue("false") boolean requireConsistency,
        @FormParam("replicated") @DefaultValue("true") boolean replicated,
        @FormParam("rowType") @DefaultValue("primary") String rowType) {
        String rendered = soyService.renderPlugin(partitions, new AmzaPartitionsPluginRegionInput(action,
            ringName,
            indexClassName,
            partitionName,
            consistency,
            requireConsistency,
            replicated,
            RowType.valueOf(rowType)));
        return Response.ok(rendered).build();
    }
}
