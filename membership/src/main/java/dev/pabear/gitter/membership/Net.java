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
  private Metric metric;
  private Double dropMsgRate;

  public Net(Config config, Metric metric) {
    this.port = Integer.parseInt(config.getConfigs().get(ConfigKeys.PORT));
    String dropRateConfig = config.getConfigs().get(ConfigKeys.DROP_MSG_RATE);
    if (dropRateConfig != null && !dropRateConfig.isEmpty()) {
      this.dropMsgRate = Double.valueOf(dropRateConfig);
    }
    this.metric = metric;
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
              ctx.response().send();
            });
    router
        .route("/metric")
        .handler(
            ctx -> {
              ctx.response()
                  .putHeader("content-type", "application/json")
                  .send(Json.encode(metric.getMetric()));
            });

    server.requestHandler(router).listen(port);
  }

  public void sendMsg(Msg msg) {
    metric.countSendMsg();
    Member member = msg.getTarget();
    client
        .post(member.getPort(), member.getIp(), "/c")
        .sendJson(msg)
        .onComplete(
            result -> {
              if (result.failed()) {
                log.error("send msg failed! msg: {}.", msg, result.cause());
              }
            });
  }

  private void receiveMsg(Msg msg) {
    metric.countReceiveMsg();
    if (dropMsgRate != null) {
      if (Math.random() < dropMsgRate) {
        metric.countDropMsg();
        return;
      }
    }
    eventBus.send(RECEIVE_TOPIC, Json.encode(msg));
  }
}
