package dev.pabear.gitter.entity;

public enum MemberStatus {
  ALIVE(0),
  SUSPECT(1),
  FAULTY(2);

  private int value;

  MemberStatus(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
