package dev.pabear.gitter.membership;

import static dev.pabear.gitter.membership.FailureDetector.TICK;

import io.vertx.core.AbstractVerticle;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Metric extends AbstractVerticle {
  private final ConcurrentLinkedQueue<Map<String, Integer>> metricQueue =
      new ConcurrentLinkedQueue<>();
  private final AtomicInteger receiveMsgCount = new AtomicInteger();
  private final AtomicInteger dropMsgCount = new AtomicInteger();
  private final AtomicInteger sendMsgCount = new AtomicInteger();
  private final AtomicInteger sendUpdateMemberCount = new AtomicInteger();
  private final AtomicInteger receiveMemberUpdateCount = new AtomicInteger();
  private final AtomicInteger memberCount = new AtomicInteger();

  public void countReceiveMsg() {
    this.receiveMsgCount.getAndIncrement();
  }

  public void countDropMsg() {
    this.receiveMsgCount.getAndIncrement();
  }

  public void countSendMsg() {
    this.sendMsgCount.getAndIncrement();
  }

  public void setSendUpdateMemberCount(int count) {
    this.sendUpdateMemberCount.set(count);
  }

  public void setMemberCount(int count) {
    this.memberCount.set(count);
  }

  public void countReceiveMemberUpdate() {
    this.receiveMemberUpdateCount.getAndIncrement();
  }

  public Map<String, Integer> getMetric() {
    return metricQueue.poll();
  }

  @Override
  public void start() {
    getVertx()
        .eventBus()
        .consumer(
            TICK,
            msg -> {
              Map<String, Integer> metrics = new HashMap<>();
              metrics.put("p", Integer.parseInt(msg.body().toString()));
              metrics.put("rc", receiveMsgCount.getAndSet(0));
              metrics.put("sc", sendMsgCount.getAndSet(0));
              metrics.put("dc", dropMsgCount.getAndSet(0));
              metrics.put("mc", memberCount.getAndSet(0));
              metrics.put("rmuc", receiveMemberUpdateCount.getAndSet(0));
              metrics.put("smuc", sendUpdateMemberCount.getAndSet(0));
              if (metricQueue.size() > 100) {
                metricQueue.poll();
              }
              metricQueue.add(metrics);
            });
  }
}
