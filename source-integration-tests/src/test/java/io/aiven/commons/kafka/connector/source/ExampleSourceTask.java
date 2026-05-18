package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import java.io.IOException;
import java.util.Map;

public class ExampleSourceTask extends AbstractSourceTask {
  private OffsetManager offsetManager;

  @Override
  protected EvolvingSourceRecordIterator getIterator(SourceCommonConfig config) {
    try {
      NativeSourceData<String> nativeSource = new ExampleNativeSourceData(config, offsetManager);
      return new EvolvingSourceRecordIterator(config, nativeSource);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected SourceCommonConfig configure(Map<String, String> props, OffsetManager offsetManager) {
    this.offsetManager = offsetManager;
    return new ExampleSourceConfig(props);
  }

  @Override
  protected void closeResources() {}

  @Override
  public String version() {
    return "";
  }
}
