package dev.pabear.gitter.membership;

import static dev.pabear.gitter.membership.Net.RECEIVE_TOPIC;
import static dev.pabear.gitter.membership.Net.SEND_TOPIC;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.MemberStatus;
import dev.pabear.gitter.entity.Msg;
import dev.pabear.gitter.entity.MsgType;
import dev.pabear.gitter.entity.Payload;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import java.net.Inet4Address;
import java.net.UnknownHostException;
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
  private final int port;
  private int incarnation = 0;

  private String myIp = Inet4Address.getLocalHost().toString();
  private HashMap<String, Member> ipPortMemberMap = new HashMap<>();
  private Stack<Member> memberPingOrder = new Stack<>();
  private HashMap<String, Member> ipPortUpdatedMembers = new HashMap<>();
  private HashMap<String, Member> ipPortUpdatedMembersForSend = new HashMap<>();

  private long periodId;
  private Map<String, Msg> idMsgMap = new HashMap<>();
  private Map<String, Long> msgIdPingTimerMap = new HashMap<>();
  private Map<String, Long> msgIdPingReqTimerMap = new HashMap<>();
  private Map<String, Long> ipPortSuspectTimerMap = new HashMap<>();
  private Dissemination dissemination;

  FailureDetector(Config config, Dissemination dissemination) throws UnknownHostException {
    this.pingCount = Integer.parseInt(config.getConfigs().get(ConfigKeys.EACH_TURN_PING_COUNT));
    this.pingTimeout = Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_TIMEOUT));
    this.pingReqMemberCount =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_REQ_MEMBER_COUNT));
    this.pingReqTimeout = Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_REQ_TIMEOUT));
    this.suspectPeriodCount =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.SUSPECT_PERIOD_COUNT));
    this.protocolPeriod = Integer.parseInt(config.getConfigs().get(ConfigKeys.PROTOCOL_PERIOD));
    this.port = Integer.parseInt(config.getConfigs().get(ConfigKeys.PORT));
    this.dissemination = dissemination;
    if (config.getSource() != null) {
      ipPortMemberMap.put(config.getSource().IpPortString(), config.getSource());
    }
  }

  @Override
  public void start() {
    getVertx()
        .setPeriodic(
            protocolPeriod,
            periodId -> {
              this.periodId = periodId;
              this.ipPortUpdatedMembersForSend = this.ipPortUpdatedMembers;
              this.ipPortUpdatedMembers = new HashMap<>();
              for (int i = 0; i < pingCount; i++) {
                Member aPingTarget = getAPingTarget();
                if (aPingTarget == null) {
                  return;
                }
                ping(aPingTarget);
              }
            });
    getVertx()
        .eventBus()
        .consumer(
            RECEIVE_TOPIC, msg -> processMsg(Json.decodeValue((String) msg.body(), Msg.class)));
  }

  private Member getAPingTarget() {

    if (memberPingOrder.empty()) {
      List<Member> membersTemp = new LinkedList<>(ipPortMemberMap.values());
      Collections.shuffle(membersTemp);
      memberPingOrder.addAll(membersTemp);
    }
    if (memberPingOrder.empty()) {
      return null;
    }
    return memberPingOrder.pop();
  }

  private void ping(Member target) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING, id, null, target);
    sendMsg(pingMsg);
    setPingTimer(id, target);
  }

  private void pingRequest(String originMsgId, Member target) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING_REQ, id, originMsgId, target);
    sendMsg(pingMsg);
    setPingRequestTimer(id, target);
  }

  private void onPing(Msg msg) {
    updateMember(msg.getFrom());
    sendMsg(buildMsg(MsgType.ACK, msg.getId(), null, msg.getFrom()));
  }

  private void onPingReq(Msg msg) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING, id, msg.getId(), msg.getTarget());
    sendMsg(pingMsg);
    setPingTimer(id, msg.getTarget());
  }

  private void onAck(Msg msg) {
    clearPingTimer(msg.getId());
    setMemberAliveByIpPort(msg.fromIpPortString());

    Msg originMsg = idMsgMap.get(msg.getId());
    setMemberAliveByIpPort(originMsg.targetIpPortString());

    dissemination.clearSendBox();

    if (originMsg.getType() == MsgType.PING_REQ) {
      // from me
      clearPingRequestTimer(msg.getPingReqId());
    } else if (originMsg.getPingReqId() != null) {
      sendMsg(buildMsg(MsgType.ACK, originMsg.getPingReqId(), null, originMsg.getFrom()));
    }
  }

  private Msg buildMsg(MsgType type, String id, String originId, Member target) {
    Msg msg = new Msg();
    msg.setType(type);
    msg.setTarget(target);
    msg.setId(id);
    msg.setPayloads(dissemination.getPayloadFromSendBox());
    msg.setMembers(new ArrayList<>(ipPortUpdatedMembersForSend.values()));
    msg.setPingReqId(originId);
    Member member = new Member();
    member.setIp(myIp);
    member.setPort(port);
    member.setStatus(MemberStatus.ALIVE);
    member.setIncarnation(incarnation);
    msg.setFrom(member);
    return msg;
  }

  public void processMsgInfo(Msg msg) {
    processMembers(msg.getMembers());
    processPayloads(msg.getPayloads());
  }

  private void processMembers(List<Member> members) {

    for (Member member : members) {
      updateMember(member);
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
              for (int i = 0; i < Math.min(pingReqMemberCount, ipPortMemberMap.size()); i++) {
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
              setSuspectTimer(target.IpPortString());
              Member member = ipPortMemberMap.get(target.IpPortString());
              if (member == null) {
                return;
              }
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
              if (member == null) {
                return;
              }
              member.setStatus(MemberStatus.FAULTY);
              updateMember(member);
            });

    ipPortSuspectTimerMap.put(memberIpPort, tid);
  }

  private void setMemberAliveByIpPort(String ipPort) {
    Member member = ipPortMemberMap.get(ipPort);
    if (member == null || member.getStatus() == MemberStatus.ALIVE) return;
    member.setStatus(MemberStatus.ALIVE);
    updateMember(member);
  }

  private void updateMember(Member member) {
    Member oldData = ipPortUpdatedMembers.get(member.IpPortString());
    if (oldData == null) {
      if (member.getStatus() == MemberStatus.ALIVE) {
        ipPortUpdatedMembers.put(member.IpPortString(), member);
        ipPortMemberMap.put(member.IpPortString(),member);
      }
    } else {
      if (oldData.getStatus() == MemberStatus.FAULTY) {
        return;
      }
      ipPortMemberMap.put(member.IpPortString(),member);

      MemberStatus status = member.getStatus();
      if (status == MemberStatus.FAULTY) {
        ipPortUpdatedMembers.put(member.IpPortString(), member);
        ipPortMemberMap.remove(member.IpPortString());
      } else if (status == MemberStatus.SUSPECT) {
        if (oldData.getStatus() == MemberStatus.SUSPECT
            && member.getIncarnation() > oldData.getIncarnation()) {
          ipPortUpdatedMembers.put(member.IpPortString(), member);
        } else if (oldData.getStatus() == MemberStatus.ALIVE
            && member.getIncarnation() >= oldData.getIncarnation()) {
          ipPortUpdatedMembers.put(member.IpPortString(), member);
        }
      } else if (status == MemberStatus.ALIVE) {
        if (oldData.getStatus() == MemberStatus.SUSPECT
            && member.getIncarnation() > oldData.getIncarnation()) {
          ipPortUpdatedMembers.put(member.IpPortString(), member);
        } else if (oldData.getStatus() == MemberStatus.ALIVE
            && member.getIncarnation() > oldData.getIncarnation()) {
          ipPortUpdatedMembers.put(member.IpPortString(), member);
        }
      }
    }
  }

  private boolean notMe(Member member) {
    return true;
  }

  private void sendMsg(Msg msg) {
    idMsgMap.put(msg.getId(), msg);
    getVertx().eventBus().send(SEND_TOPIC, Json.encode(msg));
  }

  private void processMsg(Msg msg) {
    processMsgInfo(msg);
    if (msg.getType() == MsgType.PING) {
      onPing(msg);
    } else if (msg.getType() == MsgType.ACK) {
      onAck(msg);
    } else if (msg.getType() == MsgType.PING_REQ) {
      onPingReq(msg);
    }
  }

  public List<Member> getMembers() {
    return new ArrayList<>(ipPortMemberMap.values());
  }
}
