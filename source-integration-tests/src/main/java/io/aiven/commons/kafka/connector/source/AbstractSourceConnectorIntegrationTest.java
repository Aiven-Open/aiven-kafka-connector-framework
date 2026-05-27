/*
    Copyright 2026 Aiven Oy and project contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

   SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.commons.kafka.connector.source;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.testkit.KafkaManager;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abstract base class for the connector integration tests.
 *
 * <p>Utilizes a {@link TestConfig} to configure the connector under test as well as the consumer of
 * the messages from Kafka used to validate the data.
 *
 * @param <K> the native key type for the connector.
 * @param <V> the native value object for the connector.
 */
public abstract class AbstractSourceConnectorIntegrationTest<K extends Comparable<K>, V>
    extends AbstractSourceIntegrationBase<K, V> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSourceConnectorIntegrationTest.class);

  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(5);

  /** The TaskConfig for this test. */
  protected TestConfig testConfig;

  private List<SourceStorage.TestData> testData;
  private List<SourceStorage.TestData> firstSet;
  private List<SourceStorage.TestData> secondSet;

  /** Constructor. */
  protected AbstractSourceConnectorIntegrationTest() {}

  /**
   * Gets the test configuration for this execution.
   *
   * @return the Test configuration for this execution.
   */
  protected abstract TestConfig getTestConfig();

  @BeforeEach
  void createStorage() {
    getSourceStorage().createStorage(getTopic());
    testConfig = getTestConfig();
  }

  @AfterEach
  void removeStorage() {
    getSourceStorage().removeStorage();
    testData = null;
    firstSet = null;
    secondSet = null;
  }

  @Override
  protected Duration getOffsetFlushInterval() {
    return Duration.ofMillis(500); // half a second between flushes
  }

  /**
   * Start the connector and wait for the storage to contain the results.
   *
   * @param writeResults the results for the storage to contain.
   * @param topic the topic to read send to.
   * @param connectorConfig the connector config.
   * @throws IOException on IO Error
   * @throws ExecutionException on kafka startup error
   * @throws InterruptedException on kafka startup interruption.
   */
  private void startConnector(
      final List<SourceStorage.WriteResult> writeResults,
      final String topic,
      final Map<String, String> connectorConfig)
      throws IOException, ExecutionException, InterruptedException {
    // Start the Connector

    final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());
    kafkaManager.createTopic(topic);
    kafkaManager.configureConnector(getConnectorName(), connectorConfig);

    // verify the records were written to storage.
    waitForStorage(
        WRITE_TIMEOUT,
        () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
        nativeKeys(writeResults));
  }

  private void setupData() {
    testData = testConfig.getTestData(4);
    firstSet = testData.subList(0, 2);
    secondSet = testData.subList(2, 4);
  }

  @Test
  void testMessagesRead() throws IOException {
    final String topic = getTopic();
    final TestConfig testConfig = getTestConfig();

    LOGGER.info("Executing test: {}", testConfig.getName());

    KafkaManager kafkaManager = setupKafka(testConfig.consumerConfiguration());
    kafkaManager.createTopic(topic);

    // Map<String, String> config = getSourceStorage().createConnectorConfig();
    Map<String, String> config = testConfig.consumerConfiguration();
    CommonConfigFragment.setter(config).maxTasks(1);
    SourceConfigFragment.setter(config).targetTopic(topic);

    LOGGER.info("{}", config);

    kafkaManager.configureConnector(getTopic(), config);

    List<SourceStorage.TestData> testData = testConfig.getTestData(5);
    List<SourceStorage.WriteResult> writeResults = testConfig.writeTestData(getTopic(), testData);

    waitForStorage(
        Duration.ofSeconds(10),
        () -> getSourceStorage().getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
        nativeKeys(writeResults));

    // Poll messages from the Kafka topic and verify the consumed data
    testConfig.consumeMessages(
        messageConsumer(), topic, testData, writeResults, Duration.ofSeconds(10));
  }

  /**
   * Verify that the offset manager can read the data and skip already read messages. This tests
   * verifies that data written before a restart but not read are read after the restart.
   */
  @Test
  void writeDuringPauseReadsNewRecordsTest() {

    final String topic = getTopic();

    setupData();

    // Write 2 records  storage
    List<SourceStorage.WriteResult> writeResults = testConfig.writeTestData(topic, firstSet);

    try {
      startConnector(writeResults, topic, createConfig(topic, testConfig.initialConfig()));

      testConfig.consumeMessages(
          messageConsumer(), topic, firstSet, writeResults, Duration.ofSeconds(90));

      getKafkaManager().pauseConnector(getConnectorName());

      // write rest of data
      writeResults = testConfig.writeTestData(topic, secondSet);

      // resume the connector.
      getKafkaManager().resumeConnector(getConnectorName());

      // connector should skip the records that were previously read.
      testConfig.consumeMessages(
          messageConsumer(), topic, secondSet, writeResults, Duration.ofSeconds(90));

    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  @Test
  void writeReadsNewRecordsTest() {

    final String topic = getTopic();

    setupData();

    // Write 2 records  storage
    List<SourceStorage.WriteResult> writeResults = testConfig.writeTestData(topic, firstSet);

    try {
      startConnector(writeResults, topic, createConfig(topic, testConfig.initialConfig()));

      testConfig.consumeMessages(
          messageConsumer(), topic, firstSet, writeResults, Duration.ofSeconds(90));

      // write rest of data
      writeResults = testConfig.writeTestData(topic, secondSet);

      // connector should skip the records that were previously read.
      testConfig.consumeMessages(
          messageConsumer(), topic, secondSet, writeResults, Duration.ofSeconds(90));
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  /**
   * Verify that the offset manager can read the data and skip already read messages. This tests
   * verifies that data written after restart are read on a subsequent read but that earlier data is
   * not.
   */
  @Test
  void writeAfterRestartReadsNewRecordsTest() {
    final String topic = getTopic();
    setupData();

    // Write the data to storage
    List<SourceStorage.WriteResult> writeResults = testConfig.writeTestData(topic, firstSet);

    try {
      startConnector(writeResults, topic, createConfig(topic, testConfig.initialConfig()));

      testConfig.consumeMessages(
          messageConsumer(), topic, firstSet, writeResults, Duration.ofSeconds(10));

      getKafkaManager().restartConnector(getConnectorName());

      // write new data
      writeResults = testConfig.writeTestData(topic, secondSet);

      // verify only new records are read.
      testConfig.consumeMessages(
          messageConsumer(), topic, secondSet, writeResults, Duration.ofSeconds(20));
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  @Test
  void zeroLengthInputIsIgnoredTest() {
    final String topic = getTopic();

    List<SourceStorage.TestData> standardData = List.of(new SourceStorage.TestData(null, null));
    // Write the data to storage
    final List<SourceStorage.WriteResult> writeResults =
        testConfig.writeTestData(topic, standardData);

    try {
      startConnector(writeResults, topic, createConfig(topic, testConfig.initialConfig()));
      if (getSourceStorage().nullDataIsNullRecord()) {

        assertThatThrownBy(
                () ->
                    testConfig.consumeMessages(
                        messageConsumer(),
                        topic,
                        standardData,
                        writeResults,
                        Duration.ofSeconds(30)))
            .isInstanceOf(org.awaitility.core.ConditionTimeoutException.class)
            .hasMessageContaining("Expected size: 1 but was: 0");
      } else {
        testConfig.consumeMessages(
            messageConsumer(), topic, standardData, writeResults, Duration.ofSeconds(20));
      }
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }
}
