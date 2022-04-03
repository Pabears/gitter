package dev.pabear.gitter.entity;

import lombok.Data;

@Data
public class Member {
  private MemberStatus status;
  private String ip;
  private int port;
  private int incarnation;

  public static Member copyData(Member old) {
    Member member = new Member();
    member.setStatus(old.getStatus());
    member.setPort(old.getPort());
    member.setIp(old.getIp());
    member.setIncarnation(old.getIncarnation());
    return member;
  }

  public boolean equals(Member member) {
    return this.ipPortString().equals(member.ipPortString())
        && member.getStatus() == this.getStatus()
        && member.getIncarnation() == this.getIncarnation();
  }

  public String ipPortString() {
    return getIp() + ":" + getPort();
  }
}
