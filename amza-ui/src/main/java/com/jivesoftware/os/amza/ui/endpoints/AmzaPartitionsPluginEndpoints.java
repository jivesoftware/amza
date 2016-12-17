package com.jivesoftware.os.amza.ui.endpoints;

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
        String rendered = soyService.renderPlugin(
            partitions,
            new AmzaPartitionsPluginRegionInput());

        return Response.ok(rendered).build();
    }

    @POST
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(
        @FormParam("action") @DefaultValue("") String action,
        @FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("name") @DefaultValue("") String partitionName,
        @FormParam("durability") @DefaultValue("fsync_async") String durability,
        @FormParam("tombstoneTimestampAgeInMillis") @DefaultValue("0") long tombstoneTimestampAgeInMillis,
        @FormParam("tombstoneTimestampIntervalMillis") @DefaultValue("0") long tombstoneTimestampIntervalMillis,
        @FormParam("tombstoneVersionAgeInMillis") @DefaultValue("0") long tombstoneVersionAgeInMillis,
        @FormParam("tombstoneVersionIntervalMillis") @DefaultValue("0") long tombstoneVersionIntervalMillis,
        @FormParam("ttlTimestampAgeInMillis") @DefaultValue("0") long ttlTimestampAgeInMillis,
        @FormParam("ttlTimestampIntervalMillis") @DefaultValue("0") long ttlTimestampIntervalMillis,
        @FormParam("ttlVersionAgeInMillis") @DefaultValue("0") long ttlVersionAgeInMillis,
        @FormParam("ttlVersionIntervalMillis") @DefaultValue("0") long ttlVersionIntervalMillis,
        @FormParam("forceCompactionOnStartup") @DefaultValue("false") boolean forceCompactionOnStartup,
        @FormParam("consistency") @DefaultValue("leader_quorum") String consistency,
        @FormParam("requireConsistency") @DefaultValue("false") boolean requireConsistency,
        @FormParam("replicated") @DefaultValue("false") boolean replicated,
        @FormParam("disabled") @DefaultValue("false") boolean disabled,
        @FormParam("rowType") @DefaultValue("snappy_primary") String rowType,
        @FormParam("indexClassName") @DefaultValue("lab") String indexClassName,
        @FormParam("maxValueSizeInIndex") @DefaultValue("-1") int maxValueSizeInIndex,
        @FormParam("updatesBetweenLeaps") @DefaultValue("-1") int updatesBetweenLeaps,
        @FormParam("maxLeaps") @DefaultValue("-1") int maxLeaps) {
        String rendered = soyService.renderPlugin(partitions, new AmzaPartitionsPluginRegionInput(
            action,
            ringName,
            partitionName,
            durability,
            tombstoneTimestampAgeInMillis,
            tombstoneTimestampIntervalMillis,
            tombstoneVersionAgeInMillis,
            tombstoneVersionIntervalMillis,
            ttlTimestampAgeInMillis,
            ttlTimestampIntervalMillis,
            ttlVersionAgeInMillis,
            ttlVersionIntervalMillis,
            forceCompactionOnStartup,
            consistency,
            requireConsistency,
            replicated,
            disabled,
            rowType,
            indexClassName,
            maxValueSizeInIndex,
            null, // indexProperties
            updatesBetweenLeaps,
            maxLeaps));
        return Response.ok(rendered).build();
    }

}
