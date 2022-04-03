package dev.pabear.gitter.entity;

import java.util.List;
import lombok.Data;

@Data
public class Msg {
  private String id;
  private String pingReqId;
  private Member from;
  private Member target;
  private MsgType type;
  private List<Member> members;
  private List<Payload> payloads;

  public String getTargetIpPortString() {
    return target.getIpPortString();
  }

  public String getFromIpPortString() {
    return from.getIpPortString();
  }
}
