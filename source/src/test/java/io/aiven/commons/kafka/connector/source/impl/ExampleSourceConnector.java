package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExampleSourceConnector extends Connector {
	private Map<String, String> props;

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ExampleSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		CommonConfigFragment.setter(props).maxTasks(maxTasks);
		List<Map<String, String>> configs = new ArrayList<>(maxTasks);
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> taskConfig = new HashMap<>(props);
			CommonConfigFragment.setter(taskConfig).taskId(i);
			configs.add(taskConfig);
		}
		return configs;
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return new SourceCommonConfig.SourceCommonConfigDef();
	}

	@Override
	public String version() {
		return "Example version 1.0";
	}
}
