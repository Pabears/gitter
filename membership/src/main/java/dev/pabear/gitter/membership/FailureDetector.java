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
import java.util.Map.Entry;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailureDetector extends AbstractVerticle {
  public static final String TICK = "TICK";
  private final int pingCount;
  private final int pingTimeout;
  private final int pingReqMemberCount;
  private final int pingReqTimeout;
  private final int suspectPeriodCount;
  private final int protocolPeriod;
  private final int port;
  private final Metric metric;
  private AtomicInteger incarnation = new AtomicInteger();
  private AtomicLong periodCount = new AtomicLong();
  private Member source;

  private String myIp = Inet4Address.getLocalHost().getHostAddress();
  private Map<String, Member> ipPortMemberMap = new HashMap<>();
  private Stack<Member> memberPingOrder = new Stack<>();
  private Map<String, Member> ipPortUpdatedMembers = new HashMap<>();
  private Map<String, Member> ipPortUpdatedMembersForSend = new HashMap<>();

  private long periodId;
  private Map<String, Msg> idMsgMap = new HashMap<>();
  private Map<String, Long> msgIdTimerMap = new HashMap<>();
  private Map<String, Long> ipPortSuspectTimerMap = new HashMap<>();
  private Dissemination dissemination;

  FailureDetector(Config config, Dissemination dissemination, Metric metric)
      throws UnknownHostException {
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
    this.metric = metric;
    if (config.getSource() != null && !config.getSource().ipPortString().equals(myIpPortString())) {
      source = config.getSource();
    }
  }

  @Override
  public void start() {
    getVertx()
        .setPeriodic(
            protocolPeriod,
            periodId -> {
              this.periodId = periodId;
              clearMemberUpdateForSend();
              metric.setSendUpdateMemberCount(ipPortUpdatedMembersForSend.size());
              metric.setMemberCount(ipPortMemberMap.size());
              getVertx().eventBus().send(TICK, periodCount.getAndIncrement());
              for (int i = 0; i < pingCount; i++) {
                Member aPingTarget = getAPingTarget();
                if (aPingTarget == null || isMe(aPingTarget)) {
                  return;
                }
                ping(aPingTarget);
              }
              dissemination.clearSendBox();
            });
    getVertx()
        .eventBus()
        .consumer(
            RECEIVE_TOPIC, msg -> processMsg(Json.decodeValue((String) msg.body(), Msg.class)));
  }

  private void clearMemberUpdateForSend() {
    Map<String, Member> sent = this.ipPortUpdatedMembersForSend;
    this.ipPortUpdatedMembersForSend = this.ipPortUpdatedMembers;
    this.ipPortUpdatedMembers = new HashMap<>();
    Map<String, Member> forSend = new HashMap<>();
    for (Entry<String, Member> en : ipPortUpdatedMembersForSend.entrySet()) {
      if (sent.containsKey(en.getKey())) {
        Member win = compareMember(en.getValue(), sent.get(en.getKey()));
        if (!win.equals(sent.get(en.getKey()))) {
          forSend.put(en.getKey(), en.getValue());
        }
      }
    }
    this.ipPortUpdatedMembersForSend = forSend;
  }

  private Member getAPingTarget() {

    if (memberPingOrder.empty()) {
      ipPortMemberMap.remove(myIpPortString());
      List<Member> membersTemp = new LinkedList<>(ipPortMemberMap.values());
      Collections.shuffle(membersTemp);
      memberPingOrder.addAll(membersTemp);
    }
    if (memberPingOrder.empty()) {
      return source;
    }
    return memberPingOrder.pop();
  }

  private void ping(Member target) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING, id, null, target);
    sendMsg(pingMsg);
    setPingTimer(id, target);
  }

  private void pingRequest(Member target) {
    String id = UUID.randomUUID().toString();

    Member aPingTarget = getAPingTarget();
    if (aPingTarget == null) {
      return;
    }
    Msg pingMsg = buildMsg(MsgType.PING_REQ, id, null, aPingTarget);
    sendMsg(pingMsg);
    setPingRequestTimer(id, target);
  }

  private void onPing(Msg msg) {
    sendMsg(buildMsg(MsgType.ACK, msg.getId(), null, msg.getFrom()));
  }

  private void onPingReq(Msg msg) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING, id, msg.getId(), msg.getTarget());
    sendMsg(pingMsg);
    setPingTimer(id, msg.getTarget());
  }

  private void onAck(Msg msg) {
    clearTimerByMsgId(msg.getId());

    Msg originMsg = idMsgMap.get(msg.getId());
    if (originMsg != null && originMsg.getType() == MsgType.PING_REQ) {
      Msg originPingReq = idMsgMap.get(originMsg.getPingReqId());
      sendMsg(
          buildMsg(
              MsgType.ACK, originMsg.getId(), originMsg.getPingReqId(), originPingReq.getFrom()));
    }
  }

  private Msg buildMsg(MsgType type, String id, String originId, Member target) {
    Msg msg = new Msg();
    msg.setType(type);
    msg.setTarget(target);
    msg.setId(id);
    msg.setPayloads(new ArrayList<>(dissemination.getPayloadFromSendBox()));
    //todo first time to join, get a lot of member, than, get news.
    msg.setMembers(new ArrayList<>(ipPortMemberMap.values()));
    msg.setPingReqId(originId);
    Member member = new Member();
    member.setIp(myIp);
    member.setPort(port);
    member.setStatus(MemberStatus.ALIVE);
    member.setIncarnation(incarnation.get());
    msg.setFrom(member);
    return msg;
  }

  public void processMsgInfo(Msg msg) {
    setMemberAlive(msg.getFrom());
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
//                log.info("ping timeout, msg: {} , send pingReq.", Json.encode(idMsgMap.get(msgId)));
                pingRequest(target);
              }
            });

    msgIdTimerMap.put(msgId, tid);
  }

  private void clearTimerByMsgId(String msgId) {
    Long tid = msgIdTimerMap.get(msgId);
    if (tid == null || !getVertx().cancelTimer(tid)) {
      log.error("clear timer failed! msgId: {}.", msgId);
    }
    msgIdTimerMap.remove(msgId);
  }

  private void setPingRequestTimer(String msgId, Member target) {

    long tid =
        vertx.setTimer(
            pingReqTimeout,
            timerId -> {
              setSuspectTimer(target.ipPortString());
              Member member = ipPortMemberMap.get(target.ipPortString());
              if (member == null) {
                return;
              }
              member.setStatus(MemberStatus.SUSPECT);
              updateMember(member);
            });

    msgIdTimerMap.put(msgId, tid);
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

  private void setMemberAlive(Member member) {
    member.setStatus(MemberStatus.ALIVE);
    updateMember(member);
  }

  private void updateMember(Member member) {
    if (isMe(member)) {
      if (member.getStatus() != MemberStatus.ALIVE
          && member.getIncarnation() >= incarnation.get()) {
        Member me = new Member();
        me.setPort(port);
        me.setIp(myIp);
        me.setIncarnation(incarnation.incrementAndGet());
        me.setStatus(MemberStatus.ALIVE);
        ipPortUpdatedMembers.put(myIpPortString(), me);
      }
      return;
    }

    Member localData = ipPortMemberMap.get(member.ipPortString());
    Member comparedMember = compareMember(member, localData);
    ipPortUpdatedMembers.put(comparedMember.ipPortString(), comparedMember);
    if (comparedMember == localData && comparedMember.getStatus() == MemberStatus.FAULTY) {
      ipPortMemberMap.remove(localData.ipPortString());
    } else {
      ipPortMemberMap.put(member.ipPortString(), member);
    }
  }

  private Member compareMember(Member m1, Member m2) {

    if (m1 == null || m2 == null) {
      if (m1 == null) {
        return m2;
      } else {
        return m1;
      }
    }

    if (m1.getIncarnation() == m2.getIncarnation()) {
      if (m1.getStatus().getValue() >= m2.getStatus().getValue()) {
        return m1;
      } else {
        return m2;
      }
    } else if (m1.getIncarnation() > m2.getIncarnation()) {
      return m1;
    } else {
      return m2;
    }
  }

  private boolean isMe(Member member) {
    return myIpPortString().equals(member.ipPortString());
  }

  public String myIpPortString() {
    return this.myIp + ":" + port;
  }

  private void sendMsg(Msg msg) {
    //    log.info("send msg: {}", msg);
    if (msg.getType() != MsgType.ACK) {
      idMsgMap.put(msg.getId(), msg);
    } else {
      idMsgMap.remove(msg.getId());
    }
    getVertx().eventBus().send(SEND_TOPIC, Json.encode(msg));
  }

  private void processMsg(Msg msg) {
    if (msg.getType() != MsgType.ACK) {
      idMsgMap.put(msg.getId(), msg);
    } else {
      idMsgMap.remove(msg.getId());
    }

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
