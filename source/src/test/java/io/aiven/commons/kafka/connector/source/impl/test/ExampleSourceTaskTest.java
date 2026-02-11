/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.commons.kafka.connector.source.impl.test;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceTask;
import io.aiven.commons.kafka.connector.source.transformer.JsonTransformer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExampleSourceTaskTest {

	@Test
	void testExampleSourceTask() {

		ExampleSourceTask task = new ExampleSourceTask();
		SourceTaskContext context = mock(SourceTaskContext.class);
		when(context.offsetStorageReader()).thenReturn(mock(OffsetStorageReader.class));
		when(context.offsetStorageReader().offset(anyMap())).thenReturn(null);

		task.initialize(context);
		Map<String, String> props = new HashMap<>();

		SourceConfigFragment.setter(props).transformerClass(JsonTransformer.class).targetTopic("outputTopic");
		ConnectorCommonConfigFragment.setter(props);
		CommonConfigFragment.setter(props).maxTasks(1).taskId(0);

		task.start(props);

		List<SourceRecord> lst = new ArrayList<>();

		await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
			List<SourceRecord> l = task.poll();
			if (l != null) {
				lst.addAll(l);
			}
			assertThat(lst).hasSize(30);
		});
	}
}
