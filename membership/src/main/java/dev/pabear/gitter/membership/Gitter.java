package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.Payload;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.Json;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Gitter {
  private Dissemination dissemination;
  private FailureDetector failureDetector;
  private Metric metric;
  private Vertx vertx;

  public Gitter(Config config) throws UnknownHostException {
    log.info("start config: {}", Json.encode(config));
    this.dissemination = new Dissemination(config);
    this.metric = new Metric();
    this.failureDetector = new FailureDetector(config, dissemination, metric);
    vertx = Vertx.vertx();
    vertx.deployVerticle(failureDetector);
    vertx.deployVerticle(metric);
    Net net = new Net(config, metric);
    vertx.deployVerticle(net);
  }

  public List<Member> getMembers() {
    return failureDetector.getMembers();
  }

  public void sendPayload(Payload payload) {
    dissemination.addToSendBox(payload);
  }

  public List<Payload> getReceives() {
    return dissemination.getReceived();
  }

  public String myIpPortString() {
    return failureDetector.myIpPortString();
  }

  public Map<String, Integer> getMetric() {
    return metric.getMetric();
  }

  public void stop() {
    vertx.close();
  }
}
