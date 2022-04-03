package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Payload;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Dissemination {
  private final int payloadCountLimit;
  private List<Payload> sendBox = new ArrayList<>();
  private List<Payload> receiveBox = new ArrayList<>();
  private AtomicInteger lastIndex = new AtomicInteger();

  Dissemination(Config config) {
    this.payloadCountLimit =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.PAYLOAD_COUNT_LIMIT));
  }

  public void addToSendBox(Payload payload) {
    synchronized (payload) {
      this.sendBox.add(payload);
    }
  }

  public List<Payload> getPayloadFromSendBox() {
    if (sendBox.size() == 0) {
      return new ArrayList<>();
    }
    int count = Math.min(sendBox.size(), payloadCountLimit);
    lastIndex.set(count);
    return sendBox.subList(0, count - 1);
  }

  public void clearSendBox() {
    synchronized (sendBox) {
      if (sendBox.isEmpty()) {
        return;
      }
      if (sendBox.size() == 1) {
        sendBox.remove(0);
      } else sendBox = sendBox.subList(lastIndex.get(), sendBox.size() - 1);
    }
  }

  public void addToReceiveBox(Payload payload) {
    receiveBox.add(payload);
  }

  public void addAllToReceiveBox(List<Payload> payloads) {
    receiveBox.addAll(payloads);
  }

  public List<Payload> getReceived() {
    if (receiveBox.isEmpty()) {
      return new ArrayList<>();
    }
    return receiveBox.subList(0, receiveBox.size() - 1);
  }
}
