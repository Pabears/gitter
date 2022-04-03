package dev.pabear.gitter.entity;

import lombok.Data;

@Data
public class Member {
  private MemberStatus status;
  private String ip;
  private int port;
  private int incarnation;

  public String getIpPortString() {
    return getIp() + ":" + getPort();
  }
}
