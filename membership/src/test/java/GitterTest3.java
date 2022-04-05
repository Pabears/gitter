import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.membership.Gitter;
import io.vertx.core.Vertx;
import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GitterTest3 {
  public static void main(String[] args) throws UnknownHostException {
    Vertx vertx = Vertx.vertx();
    Config config = new Config();
    config.getConfigs().put(ConfigKeys.PORT, "8082");
    config.getConfigs().put(ConfigKeys.PROTOCOL_PERIOD, "5000");
    config.getConfigs().put(ConfigKeys.PING_TIMEOUT, "3000");
    config.getConfigs().put(ConfigKeys.EACH_TURN_PING_COUNT, "1");
    config.getConfigs().put(ConfigKeys.PING_REQ_MEMBER_COUNT, "1");
    config.getConfigs().put(ConfigKeys.PING_REQ_TIMEOUT, "3000");
    config.getConfigs().put(ConfigKeys.SUSPECT_PERIOD_COUNT, "5");
    config.getConfigs().put(ConfigKeys.PAYLOAD_COUNT_LIMIT, "2");


    Member source = new Member();
    source.setPort(8080);
    source.setIp("127.0.0.1");
    config.setSource(source);

    Gitter gitter1 = new Gitter(config);

    vertx.setPeriodic(
        Long.parseLong(config.getConfigs().get(ConfigKeys.PROTOCOL_PERIOD)),
        tid -> {
          log.info("ip: {}, metric: {}.", gitter1.myIpPortString(), gitter1.getMetric());
        });
//
//    vertx.setPeriodic(
//        30000,
//        tid -> {
//          log.warn(
//              "member : {}, received : {}.",
//              gitter1.getMembers(),
//              Json.encode(gitter1.getReceives()));
//        });
  }
}
