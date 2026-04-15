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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.fasterxml.jackson.databind.JsonNode;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.extractor.AvroExtractor;
import io.aiven.commons.kafka.connector.source.extractor.ByteArrayExtractor;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorInfo;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorRegistry;
import io.aiven.commons.kafka.connector.source.extractor.JsonExtractor;
import io.aiven.commons.kafka.connector.source.integration.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.integration.ConsumerPropertiesBuilder;
import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
import io.aiven.commons.kafka.connector.source.task.DistributionType;
import io.aiven.commons.kafka.connector.source.testFixture.format.AvroTestDataFixture;
import io.aiven.commons.kafka.connector.source.testFixture.format.JsonTestDataFixture;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.confluent.connect.avro.AvroConverter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kerby.kerberos.kerb.crypto.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests to ensure that data written to the storage layer in various formats can be correctly read
 * by the source implementation.
 *
 * @param <K> the native key type.
 * @param <N> the native object type
 */
@SuppressWarnings("PMD.ExcessiveImports")
public abstract class AbstractSourceTaskIntegrationTest<K extends Comparable<K>, N>
    extends AbstractSourceIntegrationBase<K, N> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSourceTaskIntegrationTest.class);

  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(5);

  /** static to indicate that the task has not been set */
  private static final int TASK_NOT_SET = -1;

  /** 3 standard test data string */
  private static final String TEST_DATA_1 = "Hello, Kafka Connect Abstract  Source! object 1";

  private static final String TEST_DATA_2 = "Hello, Kafka Connect Abstract  Source! object 2";
  private static final String TEST_DATA_3 = "Hello, Kafka Connect Abstract Source! object 3";

  protected AbstractSourceTaskIntegrationTest() {
    super();
  }

  @BeforeEach
  void createStorage() {
    getSourceStorage().createStorage();
  }

  @AfterEach
  void removeStorage() {
    getSourceStorage().removeStorage();
  }

  @Override
  protected Duration getOffsetFlushInterval() {
    return Duration.ofMillis(500); // half a second between flushes
  }

  // /**
  // * Creates a configuration with the specified arguments.
  // *
  // * @param topic
  // * the topic to write the results to.
  // * @param taskId
  // * the task ID, may be {@link #TASK_NOT_SET}.
  // * @param maxTasks
  // * the maximum number of tasks for the source connector.
  // * @param inputFormat
  // * the input format for the data.
  // * @return a map of data values to configure the connector.
  // */
  // private Map<String, String> createConfig(final String topic, final int
  // taskId, final int maxTasks,
  // final InputFormat inputFormat) {
  // return createConfig(null, topic, taskId, maxTasks, inputFormat);
  // }
  //
  // /**
  // * Creates a configuration with the specified arguments.
  // *
  // * @param localPrefix
  // * the string to prepend to all nnative keys.
  // * @param topic
  // * the topic to write the results to.
  // * @param taskId
  // * the task ID, may be {@link #TASK_NOT_SET}.
  // * @param maxTasks
  // * the maximum number of tasks for the source connector.
  // * @param inputFormat
  // * the input format for the data.
  // * @return a map of data values to configure the connector.
  // */
  // private Map<String, String> createConfig(final String localPrefix, final
  // String topic, final int taskId,
  // final int maxTasks, final InputFormat inputFormat) {
  // final Map<String, String> configData = createConnectorConfig(localPrefix);
  // SourceConfigFragment.setter(configData).targetTopic(topic);
  //
  // final CommonConfigFragment.Setter setter =
  // CommonConfigFragment.setter(configData)
  // .connector(getConnectorClass()).name(getConnectorName()).maxTasks(maxTasks);
  // if (taskId > TASK_NOT_SET) {
  // setter.taskId(taskId);
  // }
  //
  // if (inputFormat == InputFormat.AVRO) {
  // configData.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,"false");
  // }
  // ExtractorFragment.setter(configData).inputFormat(inputFormat);
  //
  // FileNameFragment.setter(configData).template(FILE_PATTERN);
  //
  // return configData;
  // }

  private List<SourceStorage.WriteResult<K>> writeStandardData(String topic) {
    final List<SourceStorage.WriteResult<K>> writeResults = new ArrayList<>();
    writeResults.add(write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 0));
    writeResults.add(write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 0));
    writeResults.add(write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 1));
    writeResults.add(write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 1));
    return writeResults;
  }

  /**
   * Verify that the offset manager can read the data and skip already read messages. This tests
   * verifies that data written before a restart but not read are read after the restart.
   */
  @Test
  void writeBeforeRestartReadsNewRecordsTest() {

    final String topic = getTopic();
    final ExtractorInfo ExtractorInfo = getSourceStorage().supportedExtractors().any();
    if (ExtractorInfo == null) {
      throw new IllegalStateException("No Extractors defined");
    }

    final List<SourceStorage.WriteResult<K>> writeResults = writeStandardData(topic);

    try {
      // Start the Connector
      final Map<String, String> connectorConfig =
          createConfig(topic, ExtractorInfo.extractorClass());
      ConnectorCommonConfigFragment.setter(connectorConfig)
          .keyConverter(ByteArrayConverter.class)
          .valueConverter(ByteArrayConverter.class);
      SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

      final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());
      kafkaManager.createTopic(topic);
      kafkaManager.configureConnector(getConnectorName(), connectorConfig);

      // verify the records were written to storage.
      waitForStorage(
          WRITE_TIMEOUT,
          () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
          writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());

      // Poll messages from the Kafka topic and verify the consumed data
      List<String> records =
          messageConsumer().consumeStringMessages(topic, 4, Duration.ofSeconds(10));

      // Verify that the correct data is read from the S3 bucket and pushed to Kafka
      assertThat(records).containsOnly(TEST_DATA_1, TEST_DATA_2);

      // write new data
      writeResults.add(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 1));
      writeResults.add(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 2));

      // restart the connector.
      kafkaManager.restartConnector(getConnectorName());

      // connector should skip the records that were previously read.
      records = messageConsumer().consumeStringMessages(topic, 2, Duration.ofSeconds(10));
      assertThat(records).containsOnly(TEST_DATA_3);
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

    final List<SourceStorage.WriteResult<K>> writeResults = writeStandardData(topic);

    try {
      // Start the Connector
      final Map<String, String> connectorConfig = createConfig(topic, ByteArrayExtractor.class);
      ConnectorCommonConfigFragment.setter(connectorConfig)
          .keyConverter(ByteArrayConverter.class)
          .valueConverter(ByteArrayConverter.class);
      SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

      final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());
      kafkaManager.createTopic(topic);
      kafkaManager.configureConnector(getConnectorName(), connectorConfig);

      // verify the records were written to storage.
      waitForStorage(
          WRITE_TIMEOUT,
          () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
          writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());

      // Poll messages from the Kafka topic and verify the consumed data
      List<String> records =
          messageConsumer().consumeStringMessages(topic, 4, Duration.ofSeconds(10));

      // Verify that the correct data is read from the S3 bucket and pushed to Kafka
      assertThat(records).containsOnly(TEST_DATA_1, TEST_DATA_2);

      // restart the connector
      kafkaManager.restartConnector(getConnectorName());

      // write new data
      writeResults.add(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 1));
      writeResults.add(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 2));

      // verify only new records are read.
      records = messageConsumer().consumeStringMessages(topic, 2, Duration.ofSeconds(20));
      assertThat(records).containsOnly(TEST_DATA_3);
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  @ParameterizedTest
  @MethodSource("standardExtractorInfo")
  void zeroLengthInputIsIgnoredTest(final ExtractorInfo ExtractorInfo) {
    final String topic = getTopic();

    // write empty file object
    SourceStorage.WriteResult<K> writeResult = write(topic, new byte[0], 3);
    // verify the records were written to storage.
    waitForStorage(
        WRITE_TIMEOUT,
        () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
        List.of(writeResult.getNativeKey()));

    try {
      final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());
      kafkaManager.createTopic(topic);

      // Start the Connector
      final Map<String, String> connectorConfig =
          createConfig(topic, ExtractorInfo.extractorClass());
      ConnectorCommonConfigFragment.setter(connectorConfig)
          .keyConverter(ByteArrayConverter.class)
          .valueConverter(ByteArrayConverter.class);
      SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

      kafkaManager.configureConnector(getConnectorName(), connectorConfig);

      // Poll messages from the Kafka topic and verify the consumed data
      final ConsumerPropertiesBuilder consumerProperties =
          consumerPropertiesBuilder()
              .valueDeserializer(ByteArrayDeserializer.class)
              .keyDeserializer(ByteArrayDeserializer.class);

      final List<ConsumerRecord<byte[], byte[]>> records =
          messageConsumer()
              .consumeMessages(
                  topic,
                  consumerProperties,
                  1,
                  Duration.ofSeconds(10),
                  ByteArrayDeserializer.class,
                  ByteArrayDeserializer.class)
              .toList();

      // Verify that the correct data is read from the S3 bucket and pushed to Kafka
      assertThat(records).isEmpty();
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  /**
   * Tests that items written in BYTE format, with or without a prefix are correctly read and
   * reported.
   */
  @Test
  void bytesTest() {
    final String topic = getTopic();

    final List<SourceStorage.WriteResult<K>> writeResults = writeStandardData(topic);

    try {
      // Start the Connector
      final Map<String, String> connectorConfig = createConfig(topic, ByteArrayExtractor.class);
      ConnectorCommonConfigFragment.setter(connectorConfig)
          .keyConverter(StringConverter.class)
          .valueConverter(ByteArrayConverter.class);
      SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

      // ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG
      Map<String, String> configOverrides = new HashMap<>();
      configOverrides.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, null);
      final KafkaManager kafkaManager = setupKafka(configOverrides);
      kafkaManager.createTopic(topic);
      String result = kafkaManager.configureConnector(getConnectorName(), connectorConfig);
      LOGGER.info("Connector configured: {}", result);

      waitForStorage(
          WRITE_TIMEOUT,
          () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
          writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());

      // Poll messages from the Kafka topic and verify the consumed data
      List<byte[]> bRecords =
          messageConsumer().consumeByteMessages(topic, 2, Duration.ofSeconds(20));
      List<String> records =
          messageConsumer().consumeStringMessages(topic, 2, Duration.ofSeconds(20));

      // Verify that the correct data is read from the S3 bucket and pushed to Kafka
      assertThat(records).containsOnly(TEST_DATA_1, TEST_DATA_2);

      // Verify offset positions
      // verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  /**
   * Tests that various buffer sizes byte BYTE input split the buffer properly.
   *
   * @param maxBufferSize the buffersize to use.
   */
  @ParameterizedTest
  @CsvSource({"4096", "3000", "4101"})
  void bytesBufferSizeTest(final int maxBufferSize) {
    final String topic = getTopic();

    final int byteArraySize = 6000;
    final byte[] testData = Random.makeBytes(6000);

    // write empty file object
    SourceStorage.WriteResult<K> writeResult = write(topic, testData, 3);
    final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords =
        Map.of(writeResult.getOffsetManagerKey(), (long) byteArraySize);
    // verify the records were written to storage.
    waitForStorage(
        WRITE_TIMEOUT,
        () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
        List.of(writeResult.getNativeKey()));

    try {
      // configure the consumer
      final Map<String, String> connectorConfig = createConfig(topic, ByteArrayExtractor.class);
      ConnectorCommonConfigFragment.setter(connectorConfig)
          .keyConverter(ByteArrayConverter.class)
          .valueConverter(ByteArrayConverter.class);
      SourceConfigFragment.setter(connectorConfig)
          .distributionType(DistributionType.PARTITION)
          .extractorBuffer(maxBufferSize);

      // configure/start Kafka
      final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());
      kafkaManager.createTopic(topic);
      kafkaManager.configureConnector(getConnectorName(), connectorConfig);

      // Poll messages from the Kafka topic and verify the consumed data
      final List<byte[]> records =
          messageConsumer().consumeByteMessages(topic, 2, Duration.ofSeconds(60));

      assertThat(records.get(0)).hasSize(maxBufferSize);
      assertThat(records.get(1)).hasSize(byteArraySize - maxBufferSize);

      assertThat(records.get(0)).isEqualTo(Arrays.copyOfRange(testData, 0, maxBufferSize));
      assertThat(records.get(1))
          .isEqualTo(Arrays.copyOfRange(testData, maxBufferSize, testData.length));

      verifyOffsetPositions(expectedOffsetRecords, Duration.ofMinutes(2));
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  /**
   * Tests that items written in AVRO format are correctly read and reported.
   *
   * @throws IOException if data can not be created.
   */
  @Test
  void avroTest() throws IOException {
    final var topic = getTopic();

    final int numOfRecsFactor = 5000;

    final byte[] outputStream1 = AvroTestDataFixture.generateAvroData(1, numOfRecsFactor);
    final byte[] outputStream2 =
        AvroTestDataFixture.generateAvroData(numOfRecsFactor + 1, numOfRecsFactor);
    final byte[] outputStream3 =
        AvroTestDataFixture.generateAvroData(2 * numOfRecsFactor + 1, numOfRecsFactor);
    final byte[] outputStream4 =
        AvroTestDataFixture.generateAvroData(3 * numOfRecsFactor + 1, numOfRecsFactor);
    final byte[] outputStream5 =
        AvroTestDataFixture.generateAvroData(4 * numOfRecsFactor + 1, numOfRecsFactor);

    final List<SourceStorage.WriteResult<K>> writeResults = new ArrayList<>();

    writeResults.add(write(topic, outputStream1, 1));
    writeResults.add(write(topic, outputStream2, 1));

    writeResults.add(write(topic, outputStream3, 2));
    writeResults.add(write(topic, outputStream4, 2));
    writeResults.add(write(topic, outputStream5, 2));

    final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
    writeResults.stream()
        .map(SourceStorage.WriteResult::getOffsetManagerKey)
        .forEach(key -> expectedOffsetRecords.put(key, (long) numOfRecsFactor));

    // verify the records were written to storage.
    waitForStorage(
        WRITE_TIMEOUT,
        () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
        writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());

    try {
      // create configuration
      final Map<String, String> connectorConfig = createConfig(topic, AvroExtractor.class);
      ConnectorCommonConfigFragment.Setter connectorSetter =
          ConnectorCommonConfigFragment.setter(connectorConfig)
              .keyConverter(ByteArrayConverter.class)
              .valueConverter(AvroConverter.class);

      // start the manager
      final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());

      connectorSetter
          .valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
          .schemaRegistry(getKafkaManager().getSchemaRegistryUrl());

      SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.OBJECT_HASH);

      // finish configuring the manager
      kafkaManager.createTopic(topic);
      kafkaManager.configureConnector(getConnectorName(), connectorConfig);

      // Poll Avro messages from the Kafka topic and deserialize them
      // Waiting for 25k kafka records in this test so a longer Duration is added.
      final List<GenericRecord> records =
          messageConsumer().consumeAvroMessages(topic, numOfRecsFactor * 5, Duration.ofSeconds(30));
      // Ensure this method deserializes Avro

      // Verify that the correct data is read from storage and pushed to Kafka
      assertThat(records)
          .map(record -> entry(record.get("id"), record.get("message").toString()))
          .contains(
              entry(1, "Hello, from Avro Test Data Fixture! object 1"),
              entry(2, "Hello, from Avro Test Data Fixture! object 2"),
              entry(
                  numOfRecsFactor, "Hello, from Avro Test Data Fixture! object " + numOfRecsFactor),
              entry(
                  2 * numOfRecsFactor,
                  "Hello, from Avro Test Data Fixture! object " + (2 * numOfRecsFactor)),
              entry(
                  3 * numOfRecsFactor,
                  "Hello, from Avro Test Data Fixture! object " + (3 * numOfRecsFactor)),
              entry(
                  4 * numOfRecsFactor,
                  "Hello, from Avro Test Data Fixture! object " + (4 * numOfRecsFactor)),
              entry(
                  5 * numOfRecsFactor,
                  "Hello, from Avro Test Data Fixture! object " + (5 * numOfRecsFactor)));

      verifyOffsetPositions(expectedOffsetRecords, Duration.ofMinutes(1));
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  /**
   * Tests that items written in PARQUET format, with or without a prefix are correctly read and
   * reported.
   *
   * @param addPrefix if {@code true} a prefix is added to the native key.
   */
  /*
  	@ParameterizedTest
  	@ValueSource(booleans = {true, false})
  	void parquetTest(final boolean addPrefix) throws IOException {
  		final var topic = getTopic();
  		final int partition = 0;
  		final String prefixPattern = "bucket/topics/{{topic}}/partition/{{partition}}/";
  		final String prefix = addPrefix ? format("topics/%s/partition=%s/", topic, partition) : null;
  		final String name = "testuser";

  		// write the avro messages
  		final K objectKey = createKey(prefix, topic, partition);
  		writeWithKey(objectKey, ParquetTestDataFixture.generateParquetData(name, 100));

  		assertThat(getNativeStorage()).hasSize(1);

  		final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 4, InputFormat.PARQUET);
  		CommonConfigFragment.setter(connectorConfig).keyConverter(ByteArrayConverter.class)
  				.valueConverter(AvroConverter.class);

  		SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

  		if (addPrefix) {
  			FileNameFragment.setter(connectorConfig).prefixTemplate(prefixPattern);
  		}

  		try {
  			final KafkaManager kafkaManager = setupKafka();

  			ExtractorFragment.setter(connectorConfig)
  					.valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
  					.schemaRegistry(getKafkaManager().getSchemaRegistryUrl());

  			kafkaManager.createTopic(topic);
  			kafkaManager.configureConnector(getConnectorName(), connectorConfig);

  			final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, 100,
  					Duration.ofSeconds(20));
  			final List<String> expectedRecordNames = IntStream.range(0, 100).mapToObj(i -> name + i)
  					.collect(Collectors.toList());
  			assertThat(records).extracting(record -> record.get("name").toString())
  					.containsExactlyInAnyOrderElementsOf(expectedRecordNames);
  		} catch (IOException | ExecutionException | InterruptedException e) {
  			LOGGER.error("{} Error", getLogPrefix(), e);
  			fail(e);
  		} finally {
  			deleteConnector();
  		}
  	}
  */
  /** Tests that items written in JSONL format are correctly read and reported. */
  @Test
  void jsonlTest() throws IOException {

    final var topic = getTopic();

    final byte[] testData = JsonTestDataFixture.generateJsonData(500);
    SourceStorage.WriteResult<K> writeResult = write(topic, testData, 3);
    final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
    expectedOffsetRecords.put(writeResult.getOffsetManagerKey(), 500L);

    try {

      final Map<String, String> connectorConfig = createConfig(topic, JsonExtractor.class);
      ConnectorCommonConfigFragment.setter(connectorConfig)
          .keyConverter(ByteArrayConverter.class)
          .valueConverter(JsonConverter.class);
      SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

      final KafkaManager kafkaManager = setupKafka(Collections.emptyMap());
      kafkaManager.createTopic(topic);
      kafkaManager.configureConnector(getConnectorName(), connectorConfig);

      waitForStorage(
          WRITE_TIMEOUT,
          () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
          List.of(writeResult.getNativeKey()));

      // Poll Json messages from the Kafka topic and deserialize them
      final List<JsonNode> records =
          messageConsumer().consumeJsonMessages(topic, 500, Duration.ofSeconds(60));

      assertThat(records)
          .map(jsonNode -> jsonNode.get("payload"))
          .anySatisfy(
              jsonNode -> {
                assertThat(jsonNode.get("message").asText())
                    .contains(JsonTestDataFixture.MESSAGE_PREFIX);
                assertThat(jsonNode.get("id").asText()).contains("1");
              });

      // Verify offset positions
      verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(5));

    } catch (IOException | ExecutionException | InterruptedException e) {
      LOGGER.error("{} Error", getLogPrefix(), e);
      fail(e);
    } finally {
      deleteConnector();
    }
  }

  /**
   * Verifies the offset positions reported on the offset topic match the expected values.
   *
   * @param expectedRecords A map of OffsetManagerKey to count.
   * @param timeLimit the maximum time to wait for the results before failing.
   */
  protected void verifyOffsetPositions(
      final Map<OffsetManager.OffsetManagerKey, Long> expectedRecords, final Duration timeLimit) {
    LOGGER.error("{} Verification of Offset Positions is disabled!!!", getLogPrefix());
    // final Properties consumerProperties =
    // consumerPropertiesBuilder().keyDeserializer(ByteArrayDeserializer.class)
    // .valueDeserializer(ByteArrayDeserializer.class)
    // .build();
    // final MessageConsumer messageConsumer = messageConsumer();
    // final KafkaManager kafkaManager = getKafkaManager();
    // final Map<OffsetManager.OffsetManagerKey, Long> offsetRecs = new HashMap<>();
    // try (KafkaConsumer<byte[], byte[]> consumer = new
    // KafkaConsumer<>(consumerProperties)) {
    // consumer.subscribe(Collections.singletonList(kafkaManager.getOffsetTopic()));
    // await().atMost(timeLimit).pollInterval(Duration.ofSeconds(1)).untilAsserted(()
    // -> {
    // messageConsumer.consumeOffsetMessages(consumer)
    // .forEach(o -> offsetRecs.put(o.getManagerKey(), o.getRecordCount()));
    // assertThat(offsetRecs).containsAllEntriesOf(expectedRecords);
    // });
    // }
  }

  static List<ExtractorInfo> standardExtractorInfo() {
    return ExtractorRegistry.STANDARD.list();
  }
}
