package dev.pabear.gitter.membership;

import static dev.pabear.gitter.membership.FailureDetector.TICK;

import io.vertx.core.AbstractVerticle;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Metric extends AbstractVerticle {
  private AtomicInteger receiveMsgCount = new AtomicInteger();
  private AtomicInteger sendMsgCount = new AtomicInteger();
  private AtomicInteger updateMemberCount = new AtomicInteger();
  private AtomicInteger receiveMemberUpdateCount = new AtomicInteger();

  public void countReceiveMsg() {
    this.receiveMsgCount.getAndIncrement();
  }

  public void countSendMsg() {
    this.sendMsgCount.getAndIncrement();
  }

  public void setUpdateMemberCount(int count) {
    this.updateMemberCount.set(count);
  }

  public void countReceiveMemberUpdate() {
    this.receiveMemberUpdateCount.getAndIncrement();
  }

  @Override
  public void start() {
    getVertx()
        .eventBus()
        .consumer(
            TICK,
            msg -> {
              log.warn(
                  "period count: {}, rc: {}, sc: {}, umc: {}.",
                  msg.body().toString(),
                  receiveMsgCount.getAndSet(0),
                  sendMsgCount.getAndSet(0),
                  updateMemberCount.getAndSet(0));
            });
  }
}
