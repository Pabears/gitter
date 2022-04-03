package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Msg;
import io.vertx.core.AbstractVerticle;

public class Net extends AbstractVerticle {
  private FailureDetector failureDetector;

  public Net(FailureDetector failureDetector) {
    this.failureDetector = failureDetector;
  }

  private void sendMsg() {}

  private void receiveMsg(Msg msg) {
    failureDetector.processMsgInfo(msg);
  }
}
