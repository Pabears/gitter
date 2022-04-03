package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.MemberStatus;
import dev.pabear.gitter.entity.Msg;
import dev.pabear.gitter.entity.MsgType;
import dev.pabear.gitter.entity.Payload;
import io.vertx.core.AbstractVerticle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;

public class FailureDetector extends AbstractVerticle {
  private final int pingCount;
  private final int pingTimeout;
  private final int pingReqMemberCount;
  private final int pingReqTimeout;
  private final int suspectPeriodCount;
  private final int protocolPeriod;
  private HashMap<String, Member> ipPortMemberMap = new HashMap<>();
  private Stack<Member> memberPingOrder = new Stack<>();
  private HashMap<String, Member> ipPortUpdatedMembers = new HashMap<>();
  private long periodId;
  private Map<String, Msg> idMsgMap = new HashMap<>();
  private Map<String, Long> msgIdPingTimerMap = new HashMap<>();
  private Map<String, Long> msgIdPingReqTimerMap = new HashMap<>();
  private Map<String, Long> ipPortSuspectTimerMap = new HashMap<>();
  private Dissemination dissemination;

  FailureDetector(Config config, Dissemination dissemination) {
    this.pingCount = Integer.parseInt(config.getConfigs().get(ConfigKeys.EACH_TURN_PING_COUNT));
    this.pingTimeout = Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_TIMEOUT));
    this.pingReqMemberCount =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_REQ_MEMBER_COUNT));
    this.pingReqTimeout = Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_REQ_TIMEOUT));
    this.suspectPeriodCount =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.SUSPECT_PERIOD_COUNT));
    this.protocolPeriod = Integer.parseInt(config.getConfigs().get(ConfigKeys.PROTOCOL_PERIOD));
  }

  @Override
  public void start() {
    getVertx()
        .setPeriodic(
            protocolPeriod,
            periodId -> {
              this.periodId = periodId;
              for (int i = 0; i < pingCount; i++) {
                ping(getAPingTarget());
              }
            });
  }

  private Member getAPingTarget() {

    if (memberPingOrder.empty()) {
      List<Member> membersTemp = new LinkedList<>(ipPortMemberMap.values());
      Collections.shuffle(membersTemp);
      memberPingOrder.addAll(membersTemp);
    }
    return memberPingOrder.pop();
  }

  private void ping(Member target) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING, id, null, target);
    getVertx().eventBus().send(id, pingMsg);
    setPingTimer(id, target);
  }

  private void pingRequest(String originMsgId, Member target) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING_REQ, id, originMsgId, target);
    getVertx().eventBus().send(id, pingMsg);
    setPingRequestTimer(id, target);
  }

  private void ack(Msg msg) {}

  private void pingReqAck(Msg msg) {}

  private void onPing(Msg msg) {}

  private void onPingReq(Msg msg) {}

  private void onAck(Msg msg) {
    clearPingTimer(msg.getId());
    setMemberAliveByIpPort(msg.getFromIpPortString());

    Msg originMsg = idMsgMap.get(msg.getId());
    setMemberAliveByIpPort(originMsg.getTargetIpPortString());

    if (originMsg.getType() == MsgType.PING_REQ && notMe(originMsg.getFrom())) {
      ack(buildMsg(MsgType.ACK, originMsg.getPingReqId(), null, originMsg.getFrom()));
    }
  }

  private Msg buildMsg(MsgType type, String id, String originId, Member target) {
    Msg msg = new Msg();
    msg.setType(type);
    msg.setTarget(target);
    msg.setId(id);
    msg.setPayloads(dissemination.popPayloadFromSendBox());
    msg.setMembers(new ArrayList<>(ipPortUpdatedMembers.values()));
    return msg;
  }

  public void processMsgInfo(Msg msg) {
    processMembers(msg.getMembers());
    processPayloads(msg.getPayloads());
  }

  private void processMembers(List<Member> members) {

    for (Member member : members) {
      Member origin = ipPortMemberMap.get(member.getIpPortString());
      if (origin == null || member.getIncarnation() > origin.getIncarnation()) {
        ipPortMemberMap.put(member.getIpPortString(), member);
        updateMember(member);
      }
    }
  }

  private void processPayloads(List<Payload> payloads) {
    dissemination.addAllToReceiveBox(payloads);
  }

  private void setPingTimer(String msgId, Member target) {
    long tid =
        vertx.setTimer(
            pingTimeout,
            timerId -> {
              for (int i = 0; i < pingReqMemberCount; i++) {
                pingRequest(msgId, target);
              }
            });

    msgIdPingTimerMap.put(msgId, tid);
  }

  private void clearPingTimer(String msgId) {
    Long tid = msgIdPingTimerMap.get(msgId);
    if (tid == null) {
      return;
    }
    getVertx().cancelTimer(tid);
    msgIdPingTimerMap.remove(msgId);
  }

  private void setPingRequestTimer(String msgId, Member target) {

    long tid =
        vertx.setTimer(
            pingReqTimeout,
            timerId -> {
              setSuspectTimer(target.getIpPortString());
              Member member = ipPortMemberMap.get(target.getIpPortString());
              member.setStatus(MemberStatus.SUSPECT);
              updateMember(member);
            });

    msgIdPingReqTimerMap.put(msgId, tid);
  }

  private void clearPingRequestTimer(String msgId) {
    Long tid = msgIdPingReqTimerMap.get(msgId);
    if (tid == null) return;
    getVertx().cancelTimer(tid);
    msgIdPingReqTimerMap.remove(msgId);
  }

  private void setSuspectTimer(String memberIpPort) {
    long tid =
        vertx.setTimer(
            suspectPeriodCount * protocolPeriod,
            timerId -> {
              Member member = ipPortMemberMap.get(memberIpPort);
              member.setStatus(MemberStatus.FAULTY);
              updateMember(member);
            });

    ipPortSuspectTimerMap.put(memberIpPort, tid);
  }

  private void setMemberAliveByIpPort(String ipPort) {
    Member member = ipPortMemberMap.get(ipPort);
    if (member == null || member.getStatus() == MemberStatus.ALIVE) return;
    member.setStatus(MemberStatus.ALIVE);
    // todo send alive
  }

  private void updateMember(Member member) {
    Member oldData = ipPortUpdatedMembers.get(member.getIpPortString());
    if (oldData == null) {
      ipPortUpdatedMembers.put(member.getIpPortString(), member);
    } else {
      ipPortUpdatedMembers.put(member.getIpPortString(), getNewMember(oldData, member));
    }
  }

  private Member getNewMember(Member m1, Member m2) {
    if (m1.getIncarnation() < m2.getIncarnation()) {

      // fixme
      MemberStatus status = m1.getStatus();

      if (status == MemberStatus.ALIVE) {

      } else if (status == MemberStatus.SUSPECT) {

      } else if (status == MemberStatus.FAULTY) {

      }
      return m2;
    } else {
      return m1;
    }
  }

  private boolean notMe(Member member) {
    return true;
  }
}
