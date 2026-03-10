/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.integration.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
import io.aiven.commons.kafka.connector.source.transformer.ByteArrayTransformer;
import io.aiven.commons.kafka.testkit.KafkaManager;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 * Test to verify OffsetManager and OffsetManagerKey implementations work as
 * expected
 *
 * @param <K>
 *            the native key type.
 * @param <N>
 *            the native object type
 */
public abstract class AbstractOffsetManagerIntegrationTest<K extends Comparable<K>, N>
		extends
			AbstractSourceIntegrationBase<K, N> {

	private static Logger LOGGER = LoggerFactory.getLogger(AbstractOffsetManagerIntegrationTest.class);
	/**
	 * Static to indicate that the TASK is not set.
	 */
	private static final int TASK_NOT_SET = -1;

	@BeforeEach
	void createStorage() {
		getSourceStorage().createStorage();
	}

	@AfterEach
	void removeStorage() {
		getSourceStorage().removeStorage();
	}

	/**
	 * Tests offset manager ability to read data from the Kafka context by:
	 * <ol>
	 * <li>writing 4 records</li>
	 * <li>reading them</li>
	 * <li>verifying proper values were read</li>
	 * <li>disabling the ring buffer and restarting the connector</li>
	 * <li>writing one record</li>
	 * <li>reading it</li>
	 * <li>verifying that the proper value was read</li>
	 * </ol>
	 *
	 * <p>
	 * By disabling the ring buffer the filtering of seen records falls to the
	 * OffsetManager exclusively.
	 * </p>
	 * <p>
	 * If the wrong value is read or no records are read then there was an error in
	 * the OffsetManager's attempt to read and parse the context information.
	 * </p>
	 */
	@Test
	void offsetManagerRead() {
		final String topic = getTopic();

		final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
		final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
		final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

		final Map<SourceStorage.WriteResult<K>, Long> expectedOffsetRecords = new HashMap<>();
		// write 4 objects
		expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0), 1L);
		expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0), 1L);
		expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1), 1L);
		expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1), 1L);

		try {
			// Start the Connector
			Map<String, String> props = ConnectorCommonConfigFragment
					.setter(createConfig(topic, ByteArrayTransformer.class)).keyConverter(StringConverter.class)
					.valueConverter(ByteArrayConverter.class).data();
			SourceConfigFragment.setter(props).transformerClass(ByteArrayTransformer.class);

			Map<String, String> configOverrides = new HashMap<>();
			configOverrides.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, null);

			final KafkaManager kafkaManager = setupKafka(configOverrides);
			// kafkaManager.createTopic(topic);
			kafkaManager.createTopics(List.of(topic), 1, (short) 1);
			// starts the connector here.
			kafkaManager.configureConnector(getConnectorName(), props);

			// verify the records were written to storage.
			waitForStorage(Duration.ofMinutes(2), () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
					expectedOffsetRecords.keySet().stream().map(SourceStorage.WriteResult::getNativeKey).toList());

			// Poll messages from the Kafka topic and verify the consumed data
			List<String> records = messageConsumer().consumeStringMessages(topic, 4, Duration.ofMinutes(1));

			// Verify that the correct data is read from the S3 bucket and pushed to Kafka
			assertThat(records).containsOnly(testData1, testData2);

			SourceConfigFragment.setter(props).ringBufferSize(0);

			LOGGER.info(">>>>> {} RESTARTING", getLogPrefix());
			kafkaManager.configureConnector(getConnectorName(), props);
			LOGGER.info(">>>>> {} RESTARTED", getLogPrefix());

			expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 1), 1L);

			records = messageConsumer().consumeStringMessages(topic, 1, Duration.ofSeconds(30));

			assertThat(records).containsOnly(testData3);

		} catch (IOException | ExecutionException | InterruptedException e) {
			LOGGER.error("{} Error", getLogPrefix(), e);
			fail(e);
		} finally {
			deleteConnector();
		}
	}

	@Override
	protected Duration getOffsetFlushInterval() {
		return Duration.ofSeconds(1);
	}
}
