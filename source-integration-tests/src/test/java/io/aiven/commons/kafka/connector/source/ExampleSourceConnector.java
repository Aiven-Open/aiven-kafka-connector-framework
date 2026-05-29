package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class ExampleSourceConnector extends SourceConnector {
  Map<String, String> config;

  @Override
  public void start(Map<String, String> props) {
    this.config = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ExampleSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> result = new ArrayList<>();
    for (int i = 0; i < maxTasks; ++i) {
      Map<String, String> other = new HashMap<>(config);
      CommonConfigFragment.setter(other).taskId(i);
      result.add(other);
    }
    return result;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return new SourceCommonConfig.SourceCommonConfigDef();
  }

  @Override
  public String version() {
    return "Testing version";
  }
}
