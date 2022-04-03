package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.Payload;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import java.net.UnknownHostException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Gitter {
  private Dissemination dissemination;
  private FailureDetector failureDetector;

  public Gitter(Config config) throws UnknownHostException {
    log.info("start config: {}", Json.encode(config));
    this.dissemination = new Dissemination(config);
    this.failureDetector = new FailureDetector(config, dissemination);
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(failureDetector);
    Net net = new Net(config);
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
}
