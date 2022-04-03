import dev.pabear.gitter.entity.Config;
import dev.pabear.gitter.entity.ConfigKeys;
import dev.pabear.gitter.entity.Member;
import dev.pabear.gitter.entity.Payload;
import dev.pabear.gitter.membership.Gitter;
import io.vertx.core.Vertx;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GitterTest {
  public static void main(String[] args) throws UnknownHostException {
    Config config = new Config();
    config.getConfigs().put(ConfigKeys.PORT, "1234");
    config.getConfigs().put(ConfigKeys.PROTOCOL_PERIOD, "5000");
    config.getConfigs().put(ConfigKeys.PING_TIMEOUT, "500");
    config.getConfigs().put(ConfigKeys.EACH_TURN_PING_COUNT, "1");
    config.getConfigs().put(ConfigKeys.PING_REQ_MEMBER_COUNT, "1");
    config.getConfigs().put(ConfigKeys.PING_REQ_TIMEOUT, "1500");
    config.getConfigs().put(ConfigKeys.SUSPECT_PERIOD_COUNT, "3");
    config.getConfigs().put(ConfigKeys.PAYLOAD_COUNT_LIMIT, "2");

    Gitter gitter1 = new Gitter(config);

    Member source = new Member();
    source.setPort(1234);
    source.setIp("localhost");
    config.setSource(source);

    AtomicLong atomicLong = new AtomicLong();
    for (int i = 0; i < 20; i++) {
      config.getConfigs().put(ConfigKeys.PORT, source.getPort() + i + "");

      Gitter gitter2 = new Gitter(config);
      Vertx.vertx()
          .setPeriodic(
              10000,
              tid -> {
                if (Math.random() < 0.01) {
                  Payload<Long> payload = new Payload<>();
                  payload.setContent(atomicLong.getAndIncrement());
                  gitter2.sendPayload(payload);
                }
              });
    }

    Vertx.vertx()
        .setPeriodic(
            1000,
            tid -> {
              log.warn("member count : {}.", gitter1.getMembers().size());
              log.warn("received count : {}.", gitter1.getReceives().size());
            });
  }
}
