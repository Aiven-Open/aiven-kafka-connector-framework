package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import java.util.Map;

public class ExampleSourceConfig extends SourceCommonConfig {
  /**
   * Constructor.
   *
   * @param originals the initial configuration data.
   */
  public ExampleSourceConfig(Map<String, String> originals) {
    super(new ExampleConfigDef(), originals);
  }

  public static class ExampleConfigDef extends SourceCommonConfigDef {

    ExampleConfigDef() {
      super();
      define(
          ExtendedConfigKey.builder("example.dir")
              .documentation("The directory to read/write to")
              .build());
    }
  }
}
