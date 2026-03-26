/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.commons.kafka.connector.source.extractor;

import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.SchemaAndValue;

/**
 * Extracts data from the native abstract source record to Key SchemaAndValue object and a stream of
 * SchemaAndValue objects for the values. This class is used within the EvolvingSourceRecordIterator
 * to convert the abstract source record into one or more Kafka source records.
 */
public abstract class Extractor implements AutoCloseable {
  /** The source configuration for the Kafka task. */
  protected final SourceCommonConfig config;

  /** The info for this Extractor */
  protected final ExtractorInfo info;

  /**
   * Constructor.
   *
   * @param config The configuration for the source connector.
   * @param info The ExtractorInfo for this extractor.
   */
  protected Extractor(SourceCommonConfig config, ExtractorInfo info) {
    this.config = config;
    this.info = info;
  }

  /**
   * Gets a stream of SchemaAndValue records from the input stream.
   *
   * @param sourceRecord The EvolvingSourceRecord being processed.
   * @return the stream of values for Kafka SourceRecords.
   */
  public abstract Stream<SchemaAndValue> generateRecords(final EvolvingSourceRecord sourceRecord);

  /**
   * Convert the native key into a Schema and Value for Kafka. By default, we will use the
   * toString() method to get the key
   *
   * @param evolvingSourceRecord the abstract source record to extract the keyData from.
   * @return a SchemaAndValue for the key.
   */
  public SchemaAndValue generateKeyData(final EvolvingSourceRecord evolvingSourceRecord) {
    return SchemaAndValueFactory.createSchemaAndValue(
        evolvingSourceRecord.getNativeKey().toString());
  }

  @Override
  public void close() throws Exception {
    // do nothing.
  }
}
