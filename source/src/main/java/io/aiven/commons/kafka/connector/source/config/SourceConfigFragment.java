/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.commons.kafka.connector.source.config;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.config.SinceInfo;
import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.ConfigFragment;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.config.validator.ScaleValidator;
import io.aiven.commons.kafka.connector.source.extractor.ByteArrayExtractor;
import io.aiven.commons.kafka.connector.source.extractor.Extractor;
import io.aiven.commons.kafka.connector.source.task.DistributionType;
import io.aiven.commons.util.collections.Scale;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

/** Defines properties that are shared across all Source implementations. */
public final class SourceConfigFragment extends ConfigFragment {

  static final String MAX_POLL_RECORDS = "max.poll.records";
  static final String TARGET_TOPIC = "topic";
  static final String ERRORS_TOLERANCE = "errors.tolerance";
  static final String DISTRIBUTION_TYPE = "distribution.type";

  /** The name of the ring buffer size property */
  static final String RING_BUFFER_SIZE = "ring.buffer.size";

  /** The name of the native start key property. Visible for use in logging */
  public static final String NATIVE_START_KEY = "native.start.key";

  static final String EXTRACTOR_CLASS = "extractor.class";
  static final String EXTRACTOR_BUFFER = "extractor.buffer";
  static final String EXTRACTOR_CACHE_SIZE = "extractor.cache.size";
  static final String EXTRACTOR_CSV_HEADERS_ENABLED = "extractor.csv.headers.enabled";
  static final String EXTRACTOR_CSV_HEADERS = "extractor.csv.headers";

  /**
   * Creates a Setter for this fragment.
   *
   * @param data the data map to modify.
   * @return the Setter
   */
  public static Setter setter(final Map<String, String> data) {
    return new Setter(data);
  }

  /**
   * Construct the SourceConfigFragment.
   *
   * @param dataAccess the FragmentDataAccess that this fragment is associated with.
   */
  public SourceConfigFragment(final FragmentDataAccess dataAccess) {
    super(dataAccess);
  }

  /**
   * Update the configuration definition with the properties for the source configuration.
   *
   * @param configDef the configuration to update.
   */
  public static void update(final ConfigDef configDef) {
    SinceInfo.Builder siBuilder =
        SinceInfo.builder()
            .groupId("io.aiven.commons")
            .artifactId("kafka-source-connector-framework");
    configDef
        .define(
            ExtendedConfigKey.builder(RING_BUFFER_SIZE)
                .type(ConfigDef.Type.INT)
                .defaultValue(1000)
                .validator(ConfigDef.Range.atLeast(1))
                .documentation("The number of storage key to store in the ring buffer.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(MAX_POLL_RECORDS)
                .type(ConfigDef.Type.INT)
                .defaultValue(500)
                .validator(ConfigDef.Range.atLeast(1))
                .documentation("Max poll records")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(ERRORS_TOLERANCE)
                .defaultValue(ToleranceType.NONE.name())
                .validator(
                    ConfigDef.CaseInsensitiveValidString.in(
                        Arrays.stream(ToleranceType.values())
                            .map(ToleranceType::name)
                            .toArray(String[]::new)))
                .documentation(
                    "Indicates to the connector what level of exceptions are allowed before the connector stops.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(TARGET_TOPIC)
                .validator(new ConfigDef.NonEmptyString())
                .documentation("The name of the topic to write records to.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(DISTRIBUTION_TYPE)
                .defaultValue(DistributionType.OBJECT_HASH.name())
                .validator(
                    ConfigDef.CaseInsensitiveValidString.in(
                        Arrays.stream(DistributionType.values())
                            .map(DistributionType::name)
                            .toArray(String[]::new)))
                .documentation(
                    "Based on tasks.max config and the type of strategy selected, objects are processed in distributed"
                        + " way by Kafka connect workers.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(NATIVE_START_KEY)
                .documentation(
                    "An identifier for the source connector to know which key to start processing from, on a restart it will also begin reading messages from this point as well")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(EXTRACTOR_CLASS)
                .type(ConfigDef.Type.CLASS)
                .defaultValue(ByteArrayExtractor.class)
                .validator(new ExtractorValidator())
                .documentation("Defines the class for the Extractor")
                .internalConfig(true)
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(EXTRACTOR_BUFFER)
                .type(ConfigDef.Type.INT)
                .defaultValue(4096)
                .validator(ScaleValidator.between(4096, Integer.MAX_VALUE, Scale.IEC))
                .documentation(
                    "Defines the size in bytes of the extractor buffer used when reading buffered input streams.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(EXTRACTOR_CACHE_SIZE)
                .type(ConfigDef.Type.INT)
                .defaultValue(500)
                .validator(ScaleValidator.between(100, Integer.MAX_VALUE, Scale.IEC))
                .documentation(
                    "Defines the size in bytes of the extractor cache used when processing Avro based input like Avro or Parquet streams.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(EXTRACTOR_CSV_HEADERS_ENABLED)
                .type(ConfigDef.Type.BOOLEAN)
                .defaultValue("true")
                .validator(
                    ConfigDef.LambdaValidator.with(
                        (k, v) -> {
                          if (v instanceof String) {
                            ConfigDef.CaseInsensitiveValidString.in("true", "false")
                                .ensureValid(k, v);
                          }
                        },
                        ConfigDef.CaseInsensitiveValidString.in("true", "false")::toString))
                .documentation(
                    "Only valid if CSV Extractor is used. If 'true' the first line of the CSV will be a header line describing all the columns. If 'false' no header line is provided and columns will be defined as 'field0' through 'fieldN'.")
                .since(siBuilder.version("0.1.0").build())
                .build())
        .define(
            ExtendedConfigKey.builder(EXTRACTOR_CSV_HEADERS)
                .type(ConfigDef.Type.LIST)
                .documentation(
                    "Only valid if CSV Extractor is used. A comma separated list of the field names for CVS records.  Rows with more fields than there are headers will be assigned 'fieldN' names")
                .since(siBuilder.version("0.1.0").build())
                .build());
  }

  /**
   * Gets the target topic.
   *
   * @return the target topic.
   */
  public String getTargetTopic() {
    return getString(TARGET_TOPIC);
  }

  /**
   * Gets the maximum number of records to poll at one time.
   *
   * @return The maximum number of records to poll at one time.
   */
  public int getMaxPollRecords() {
    return getInt(MAX_POLL_RECORDS);
  }

  /**
   * Gets the error tolerance.
   *
   * @return the error tolerance.
   */
  public ToleranceType getErrorsTolerance() {
    return ToleranceType.valueOf(getString(ERRORS_TOLERANCE).toUpperCase(Locale.ROOT));
  }

  /**
   * Gets the distribution type
   *
   * @return the distribution type.
   */
  public DistributionType getDistributionType() {
    return DistributionType.valueOf(getString(DISTRIBUTION_TYPE).toUpperCase(Locale.ROOT));
  }

  /**
   * Gets the ring buffer size.
   *
   * @return the ring buffer size.
   */
  public int getRingBufferSize() {
    return getInt(RING_BUFFER_SIZE);
  }

  /**
   * Gets the Extractor instance for this source.
   *
   * @param config the configuration for this source.
   * @return the Extractor instance for this source.
   */
  public Extractor getExtractor(SourceCommonConfig config) {
    Class<? extends Extractor> clazz;
    Object klass = values().get(EXTRACTOR_CLASS);
    if (klass instanceof String) {
      try {
        clazz = Utils.loadClass((String) klass, Extractor.class);
      } catch (ClassNotFoundException e) {
        throw new KafkaException("Class " + klass + " cannot be found", e);
      }
    } else if (klass instanceof Class<?> && Extractor.class.isAssignableFrom((Class<?>) klass)) {
      clazz = (Class<? extends Extractor>) klass;
    } else {
      throw new KafkaException(
          "Unexpected element of type "
              + klass.getClass().getName()
              + ", expected String or Class");
    }
    try {
      return clazz.getDeclaredConstructor(SourceCommonConfig.class).newInstance(config);
    } catch (InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException e) {
      throw new KafkaException(e);
    }
  }

  /**
   * Gets the size, in bytes, of the extractor buffer in bytes. Only applies to extractors that
   * create buffered input streams.
   *
   * @return the size in bytes of the extractor buffer.
   */
  public int getExtractorBufferSize() {
    return getInt(EXTRACTOR_BUFFER);
  }

  /**
   * Gets the size, in bytes, of the extractor cache size in bytes. Only applies to extractors that
   * utilize caches like Avro or Parquet.
   *
   * @return the size in bytes of the extractor buffer.
   */
  public int getExtractorCacheSize() {
    return getInt(EXTRACTOR_CACHE_SIZE);
  }

  /**
   * Gets the nativeStartKey.
   *
   * @return the key to start consuming records from.
   */
  public String getNativeStartKey() {
    return getString(NATIVE_START_KEY);
  }

  /**
   * Gets the CSV extractor header enable flag.
   *
   * @return {@code true} if headers should be parsed from CSV input, {@code false} otherwise.
   */
  public Boolean isCsvExtractorHeaderEnabled() {
    return getBoolean(EXTRACTOR_CSV_HEADERS_ENABLED);
  }

  /**
   * Gets the list of header for the CSV input. Will override any parsed from the CSV input itself.
   *
   * @return the list of headers for the CSV input.
   */
  public List<String> getCsvExtractorHeader() {
    return getList(EXTRACTOR_CSV_HEADERS);
  }

  /** The SourceConfigFragment setter. */
  public static class Setter extends AbstractFragmentSetter<Setter> {
    /**
     * Constructor.
     *
     * @param data the data to modify.
     */
    protected Setter(final Map<String, String> data) {
      super(data);
    }

    /**
     * Set the maximum poll records.
     *
     * @param maxPollRecords the maximum number of records to poll.
     * @return this
     */
    public Setter maxPollRecords(final int maxPollRecords) {
      return setValue(MAX_POLL_RECORDS, maxPollRecords);
    }

    /**
     * Sets the error tolerance.
     *
     * @param tolerance the error tolerance
     * @return this.
     */
    public Setter errorsTolerance(final ToleranceType tolerance) {
      return setValue(ERRORS_TOLERANCE, tolerance.name());
    }

    /**
     * Sets the target topic.
     *
     * @param targetTopic the target topic.
     * @return this.
     */
    public Setter targetTopic(final String targetTopic) {
      return setValue(TARGET_TOPIC, targetTopic);
    }

    /**
     * Sets the distribution type.
     *
     * @param distributionType the distribution type.
     * @return this
     */
    public Setter distributionType(final DistributionType distributionType) {
      return setValue(DISTRIBUTION_TYPE, distributionType.name());
    }

    /**
     * Sets the ring buffer size.
     *
     * @param ringBufferSize the ring buffer size
     * @return this.
     */
    public Setter ringBufferSize(final int ringBufferSize) {
      return setValue(RING_BUFFER_SIZE, ringBufferSize);
    }

    /**
     * Sets the initial native key to start from.
     *
     * @param nativeStartKey the key to start reading new messages from.
     * @return this.
     */
    public Setter nativeStartKey(final String nativeStartKey) {
      return setValue(NATIVE_START_KEY, nativeStartKey);
    }

    /**
     * Sets the extractor class for this source.
     *
     * @param extractor the class of the Extractor for this source.
     * @return the extractor for this source.
     */
    public Setter extractorClass(final Class<? extends Extractor> extractor) {
      return setValue(EXTRACTOR_CLASS, extractor);
    }

    /**
     * Sets the extractor buffer size.
     *
     * @param bufferSize the buffer size in bytes.
     * @return this.
     */
    public Setter extractorBuffer(final int bufferSize) {
      return setValue(EXTRACTOR_BUFFER, bufferSize);
    }

    /**
     * Sets the cache size in bytes.
     *
     * @param cacheSize the cache size in bytes.
     * @return this
     */
    public Setter extractorCache(final int cacheSize) {
      return setValue(EXTRACTOR_CACHE_SIZE, cacheSize);
    }

    /**
     * Sets the header flag for the CSV extractor.
     *
     * @param state the state for the header flag.
     * @return this
     */
    public Setter csvExtractorHeadersEnabled(final boolean state) {
      return setValue(EXTRACTOR_CSV_HEADERS_ENABLED, state);
    }

    /**
     * Sets the headers for the CSV. Should be a comma separated list of field names.
     *
     * @param headers the header names.
     * @return this
     */
    public Setter csvExtractorHeaders(String headers) {
      return setValue(EXTRACTOR_CSV_HEADERS, headers);
    }
  }

  private static class ExtractorValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException("Extractor class may not be null");
      }
      try {
        Class<?> clazz =
            value instanceof Class<?> ? (Class<?>) value : Class.forName(value.toString());
        if (!Extractor.class.isAssignableFrom(clazz)) {
          throw new ConfigException("Extractor class in configuration must extend Extractor");
        }
      } catch (ClassNotFoundException e) {
        throw new ConfigException(
            "Extractor class specified in configuration not found: {}", e.getMessage());
      }
    }

    @Override
    public String toString() {
      return String.format("A class that extends %s.", Extractor.class.getCanonicalName());
    }
  }
}
