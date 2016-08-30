package com.jivesoftware.os.amzabot.deployable.endpoint;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineConfig;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.merlin.config.BindInterfaceToConfiguration;

@Singleton
@Path("/api/amzabot/v1")
public class AmzaBotCoalmineEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotCoalmineService service;

    AmzaBotCoalmineEndpoints(@Context AmzaBotCoalmineService service) {
        this.service = service;
    }

    @POST
    @Consumes("application/json")
    @Path("/newminer")
    public Response newCoalminer(AmzaBotCoalmineRequest request) {
        try {
            AmzaBotCoalmineConfig config =
                BindInterfaceToConfiguration.bindDefault(AmzaBotCoalmineConfig.class);
            if (request != null) {
                config = request.genConfig();
            }

            ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("amzabot-coalmine-%d").build());
            executor.submit(service.newMinerWithConfig(config));

            return Response.accepted().build();
        } catch (Exception e) {
            LOG.error("Failed to start new coalminer", e);
            return Response.serverError().build();
        }
    }

}
