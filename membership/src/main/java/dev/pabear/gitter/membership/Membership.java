package dev.pabear.gitter.membership;

import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.MemberStatus;
import io.vertx.core.AbstractVerticle;
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
import java.util.concurrent.atomic.AtomicInteger;

public class Membership extends AbstractVerticle {
  private final int port;
  private final Metric metric;
  private AtomicInteger incarnation = new AtomicInteger();
  private Map<String, Member> ipPortMemberMap = new HashMap<>();
  private Stack<Member> memberPingOrder = new Stack<>();
  private Map<String, Member> ipPortUpdatedMembers = new HashMap<>();
  private Map<String, Member> ipPortUpdatedMembersForSend = new HashMap<>();
  private Member source;
  private String myIp = Inet4Address.getLocalHost().getHostAddress();

  public Membership(Config config, Metric metric) throws UnknownHostException {
    this.metric = metric;
    this.port = Integer.parseInt(config.getConfigs().get(ConfigKeys.PORT));
    if (config.getSource() != null && !config.getSource().ipPortString().equals(myIpPortString())) {
      source = config.getSource();
    }
  }

  public Member getMe() {
    Member member = new Member();
    member.setIp(myIp);
    member.setPort(port);
    member.setStatus(MemberStatus.ALIVE);
    member.setIncarnation(incarnation.get());
    return member;
  }

  public String myIpPortString() {
    return getMe().ipPortString();
  }

  public Member getAPingTarget() {

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

  public void updateMember(Member member) {
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
      metric.countReceiveMemberUpdate();
      ipPortMemberMap.put(member.ipPortString(), member);
    }
  }

  public void clearMemberUpdateForSend() {
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

  public List<Member> getMembers() {
    return new ArrayList<>(ipPortMemberMap.values());
  }

  public Member getMemberByIpPort(String ipPort) {
    return ipPortMemberMap.get(ipPort);
  }

  public List<Member> getMembersForSend() {
    // todo
    return new ArrayList<>(ipPortMemberMap.values());
  }

  public boolean isMe(Member member) {
    return myIpPortString().equals(member.ipPortString());
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
}
