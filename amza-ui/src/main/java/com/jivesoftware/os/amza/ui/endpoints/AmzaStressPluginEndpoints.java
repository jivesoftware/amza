package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.AmzaStressPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaStressPluginRegion.AmzaStressPluginRegionInput;
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
@Path("/amza/ui/stress")
public class AmzaStressPluginEndpoints {

    private final SoyService soyService;
    private final AmzaStressPluginRegion pluginRegion;

    public AmzaStressPluginEndpoints(@Context SoyService soyService, @Context AmzaStressPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AmzaStressPluginRegionInput("", false, "", 1000, 1000, "", "", 0, 0, 0, 0, 0, 0, "none", true, false, ""));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("client") @DefaultValue("false") boolean client,
        @FormParam("name") @DefaultValue("") String name,
        @FormParam("indexClassName") @DefaultValue("berkeleydb") String indexClassName,
        @FormParam("maxUpdatesBetweenCompactionHintMarker") @DefaultValue("1000") int maxUpdatesBetweenCompactionHintMarker,
        @FormParam("maxUpdatesBetweenIndexCommitMarker") @DefaultValue("1000") int maxUpdatesBetweenIndexCommitMarker,
        @FormParam("ringName") @DefaultValue("default") String ringName,
        @FormParam("regionPrefix") @DefaultValue("") String regionPrefix,
        @FormParam("ringSize") @DefaultValue("3") int ringSize,
        @FormParam("numBatches") @DefaultValue("1") int numBatches,
        @FormParam("batchSize") @DefaultValue("1") int batchSize,
        @FormParam("numPartitions") @DefaultValue("1") int numPartitions,
        @FormParam("numThreadsPerRegion") @DefaultValue("1") int numThreadsPerRegion,
        @FormParam("numKeyPrefixes") @DefaultValue("0") int numKeyPrefixes,
        @FormParam("consistency") @DefaultValue("none") String consistency,
        @FormParam("requireConsistency") @DefaultValue("false") boolean requireConsistency,
        @FormParam("orderedInsertion") @DefaultValue("false") boolean orderedInsertion,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AmzaStressPluginRegionInput(name.trim(),
                client,
                indexClassName.trim(),
                maxUpdatesBetweenCompactionHintMarker,
                maxUpdatesBetweenIndexCommitMarker,
                ringName,
                regionPrefix.trim(),
                ringSize,
                numBatches,
                batchSize,
                numPartitions,
                numThreadsPerRegion,
                numKeyPrefixes,
                consistency,
                requireConsistency,
                orderedInsertion,
                action));
        return Response.ok(rendered).build();
    }
}
