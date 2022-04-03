package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.Msg;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Net extends AbstractVerticle {
  public static final String SEND_TOPIC = "SEND";
  public static final String RECEIVE_TOPIC = "RECEIVE";
  private EventBus eventBus;
  private int port;
  private HttpServer server;
  private WebClient client;

  public Net(Config config) {
    this.port = Integer.parseInt(config.getConfigs().get(ConfigKeys.PORT));
  }

  @Override
  public void start() {
    eventBus = getVertx().eventBus();
    eventBus.consumer(SEND_TOPIC, msg -> sendMsg(Json.decodeValue((String) msg.body(), Msg.class)));
    server = vertx.createHttpServer();
    client = WebClient.create(vertx);
    Router router = Router.router(vertx);

    router
        .route("/c")
        .handler(BodyHandler.create())
        .handler(
            ctx -> {
              JsonObject msg = ctx.getBodyAsJson();
//              log.info("receive msg: {}", msg);
              receiveMsg(msg.mapTo(Msg.class));
            });

    server.requestHandler(router).listen(port);
  }

  public void sendMsg(Msg msg) {
    Member member = msg.getTarget();
    client.post(member.getPort(), member.getIp(), "/c").sendJson(msg);
  }

  private void receiveMsg(Msg msg) {
    eventBus.send(RECEIVE_TOPIC, Json.encode(msg));
  }
}
