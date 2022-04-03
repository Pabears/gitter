package dev.pabear.gitter.entity;

import java.util.Map;
import lombok.Data;

@Data
public class Config {
  private int configVersion;
  private Map<String, String> configs;
  private Member source;
}
