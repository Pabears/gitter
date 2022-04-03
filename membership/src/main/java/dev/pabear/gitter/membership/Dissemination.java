package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Payload;
import java.util.ArrayList;
import java.util.List;

public class Dissemination {
  private final int payloadCountLimit;
  private List<Payload> sendBox = new ArrayList<>();
  private List<Payload> receiveBox = new ArrayList<>();
  private int lastIndex = 0;

  Dissemination(Config config) {
    this.payloadCountLimit =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.PAYLOAD_COUNT_LIMIT));
  }

  public void addToSendBox(Payload payload) {
    synchronized (payload) {
      this.sendBox.add(payload);
    }
  }

  public List<Payload> popPayloadFromSendBox() {
    int count = Math.min(sendBox.size(), payloadCountLimit);
    lastIndex = count;
    return sendBox.subList(0, count - 1);
  }

  public void clearSendBox() {
    synchronized (sendBox) {
      sendBox = sendBox.subList(lastIndex, sendBox.size() - 1);
    }
  }

  public void addToReceiveBox(Payload payload) {
    receiveBox.add(payload);
  }

  public void addAllToReceiveBox(List<Payload> payloads) {
    receiveBox.addAll(payloads);
  }

  public List<Payload> getReceived(List<Payload> payloads) {
    return receiveBox.subList(0, receiveBox.size() - 1);
  }
}
