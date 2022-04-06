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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

  private final Metric metric;
  private final Membership membership;
  private AtomicLong periodCount = new AtomicLong();
  private long periodId;
  private Map<String, Msg> idMsgMap = new HashMap<>();
  private Map<String, Long> msgIdTimerMap = new HashMap<>();
  private Map<String, Long> ipPortSuspectTimerMap = new HashMap<>();
  private Dissemination dissemination;

  FailureDetector(
      Config config, Dissemination dissemination, Metric metric, Membership membership) {
    this.pingCount = Integer.parseInt(config.getConfigs().get(ConfigKeys.EACH_TURN_PING_COUNT));
    this.pingTimeout = Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_TIMEOUT));
    this.pingReqMemberCount =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_REQ_MEMBER_COUNT));
    this.pingReqTimeout = Integer.parseInt(config.getConfigs().get(ConfigKeys.PING_REQ_TIMEOUT));
    this.suspectPeriodCount =
        Integer.parseInt(config.getConfigs().get(ConfigKeys.SUSPECT_PERIOD_COUNT));
    this.protocolPeriod = Integer.parseInt(config.getConfigs().get(ConfigKeys.PROTOCOL_PERIOD));
    this.dissemination = dissemination;
    this.metric = metric;
    this.membership = membership;
  }

  @Override
  public void start() {
    getVertx()
        .setPeriodic(
            protocolPeriod,
            periodId -> {
              this.periodId = periodId;
              //                            clearMemberUpdateForSend();
              metric.setSendUpdateMemberCount(membership.getMembers().size());
              metric.setMemberCount(membership.getMembers().size());
              getVertx().eventBus().send(TICK, periodCount.getAndIncrement());
              for (int i = 0; i < pingCount; i++) {
                Member aPingTarget = membership.getAPingTarget();
                if (aPingTarget == null || membership.isMe(aPingTarget)) {
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

  private void ping(Member target) {
    String id = UUID.randomUUID().toString();
    Msg pingMsg = buildMsg(MsgType.PING, id, null, target);
    sendMsg(pingMsg);
    setPingTimer(id, target);
  }

  private void pingRequest(Member target) {
    String id = UUID.randomUUID().toString();

    Member aPingTarget = membership.getAPingTarget();
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
    // todo first time to join, get a lot of member, than, get news.
    msg.setMembers(new ArrayList<>(membership.getMembers()));
    msg.setPingReqId(originId);
    msg.setFrom(membership.getMe());
    return msg;
  }

  public void processMsgInfo(Msg msg) {
    setMemberAlive(msg.getFrom());
    processMembers(msg.getMembers());
    processPayloads(msg.getPayloads());
  }

  private void processMembers(List<Member> members) {

    for (Member member : members) {
      membership.updateMember(member);
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

                //                log.info("ping timeout, msg: {} , send pingReq.",
                // Json.encode(idMsgMap.get(msgId)));
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
              Member member = membership.getMemberByIpPort(target.ipPortString());
              if (member == null) {
                return;
              }
              member.setStatus(MemberStatus.SUSPECT);
              membership.updateMember(member);
            });

    msgIdTimerMap.put(msgId, tid);
  }

  private void setSuspectTimer(String memberIpPort) {
    long tid =
        vertx.setTimer(
            suspectPeriodCount * protocolPeriod,
            timerId -> {
              Member member = membership.getMemberByIpPort(memberIpPort);
              if (member == null) {
                return;
              }
              member.setStatus(MemberStatus.FAULTY);
              membership.updateMember(member);
            });

    ipPortSuspectTimerMap.put(memberIpPort, tid);
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

  private void setMemberAlive(Member member) {
    member.setStatus(MemberStatus.ALIVE);
    membership.updateMember(member);
  }
}
