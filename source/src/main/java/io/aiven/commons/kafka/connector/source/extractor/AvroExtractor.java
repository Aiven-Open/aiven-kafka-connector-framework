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

package io.aiven.commons.kafka.connector.source.extractor;

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms a stream of Avro records into individual kafka messages, one per record. */
public class AvroExtractor extends InputStreamExtractor {

  /**
   * Gets the registry information for this extractor.
   *
   * @return the registry information for this extractor.
   */
  public static ExtractorInfo info() {
    return new ExtractorInfo(
        "Avro",
        AvroExtractor.class,
        ExtractorInfo.FEATURE_NONE,
        "Accepts an Avro file-formatted input stream and creates one Kafka record for every Avro datum encountered.");
  }

  private final AvroData avroData;

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExtractor.class);

  /**
   * Constructs the AvroExtractor.
   *
   * @param config The configuration for this connector.
   */
  public AvroExtractor(final SourceCommonConfig config) {
    super(config, info());
    this.avroData = new AvroData(config.getExtractorCacheSize());
  }

  @Override
  public StreamSpliterator createSpliterator(
      final IOSupplier<InputStream> inputStreamIOSupplier,
      final long streamLength,
      final Context context) {
    return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
      private DataFileStream<GenericRecord> dataFileStream;
      private final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

      @Override
      protected void inputOpened(final InputStream input) throws IOException {
        dataFileStream = new DataFileStream<>(input, datumReader);
      }

      @Override
      public void doClose() {
        if (dataFileStream != null) {
          try {
            dataFileStream.close();
          } catch (IOException e) {
            LOGGER.error("Error closing reader: {}", e.getMessage(), e);
          }
        }
      }

      @Override
      protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
        if (dataFileStream.hasNext()) {
          final GenericRecord record = dataFileStream.next();
          action.accept(avroData.toConnectData(record.getSchema(), record));
          return true;
        }
        return false;
      }
    };
  }
}
