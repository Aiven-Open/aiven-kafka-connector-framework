package io.aiven.commons.kafka.connector.source;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The definition af a test. Since the connector may support multiple data types within the native
 * object (for example JSON format and Avro format) and may support different combinations of
 * converters, the test config defines the exact constellation of configurations to be tested.
 *
 * <p>The test config provides the configuration for the Connector under test as well as the Kafka
 * consumer that will read the messages from kafka to verify correctness.
 *
 * <p>In addition, this class also provides the validation method for the items read by the
 * consumer.
 */
public abstract class TestConfig {
  /** The name of this test. */
  private final String name;

  /**
   * Constructor.
   *
   * @param name the name of the test. This should be a human-readable name that describes the
   *     constellation of configuration options under test.
   */
  protected TestConfig(final String name) {
    this.name = name;
  }

  /**
   * Gets the test name.
   *
   * @return the test name.
   */
  public final String getName() {
    return name;
  }

  /**
   * The specific properties for the Kafka consumer that will read the results from Kafka.
   *
   * @return the map of property settings.
   */
  public abstract Map<String, String> consumerConfiguration();

  /**
   * The base configuration that is required by this test configuration, for example setting the
   * Extractor, key serializer, and/or value serializer.
   *
   * @return the map of property settings.
   */
  public abstract Map<String, String> initialConfig();

  /**
   * Generates test data for the specific configuration this TestConfig is testing. For example the
   * test may return a series of JSON records, Avro records, or CSV records depending on the type of
   * data that is expected to be processed.
   *
   * @param count the number of unique data items to create.
   * @return a list of unique data items of the required type.
   */
  public abstract List<SourceStorage.TestData> getTestData(int count);

  /**
   * Writes the test data returned from {@link #getTestData(int)} into the storage so that it may be
   * read back by the connector implementation.
   *
   * @param topic the topic that will be used during testing.
   * @param data the data elements to write.
   * @return A list of {@link io.aiven.commons.kafka.connector.source.SourceStorage.WriteResult}
   *     that identify the test data as stored in the system under test.
   */
  public abstract List<SourceStorage.WriteResult> writeTestData(
      String topic, List<SourceStorage.TestData> data);

  /**
   * Consumers the messages as stored in Kafka after the process has executed. The method allows the
   * test to read the messages as String, Byte Arrays, native Kafka messages, or any other supported
   * Kafka value. The items returned from this call should be the same values as specified by the
   * {@link SourceStorage.TestData#expected()} values returned from {@link #getTestData(int)}.
   *
   * @param messageConsumer the {@link AbstractSourceIntegrationBase.MessageConsumer} provided by
   *     the testing framework.
   * @param topic the topic to consume the messages from
   * @param testData the TestData records we are expecting of messages to consume.
   * @param writeResults the Write results from the storage write.
   * @param timeout the time limit in which to consume the messages.
   */
  public abstract void consumeMessages(
      AbstractSourceIntegrationBase.MessageConsumer messageConsumer,
      String topic,
      List<SourceStorage.TestData> testData,
      List<SourceStorage.WriteResult> writeResults,
      Duration timeout);
}
