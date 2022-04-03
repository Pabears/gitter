package dev.pabear.gitter.entity;

public enum MsgType {
  PING(0),
  ACK(1),
  PING_REQ(2);

  private int value;

  MsgType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
