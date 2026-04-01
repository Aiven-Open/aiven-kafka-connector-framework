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

       SPDX-License-Identifier: Apache-2
*/
package io.aiven.commons.kafka.connector.source.extractor;

import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a extractor to take a List of CSVRecord generated from apache csv-commons and
 * transforms that data into a SchemaAndValue Object usable by Connect to add messages to Kafka.
 *
 * <p>Assumptions:
 *
 * <ul>
 *   <li>CSV is in RFC-4180 format.
 *   <li>All columns have unique names, they have no names at all, or overriding names are provided.
 * </ul>
 *
 * <p>If columns have no names they are given the names "field0" to "fieldN".
 *
 * @see <a href=
 *     "https://www.ietf.org/archive/id/draft-shafranovich-rfc4180-bis-03.html">RFC-4180</a>
 */
public class CsvExtractor extends Extractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvExtractor.class);

  /** The schema builder */
  //  private SchemaBuilder valueSchema;

  /** the configured format for the parser */
  private final CSVFormat csvFormat;

  /**
   * Gets the registry information for this extractor.
   *
   * @return the registry information for this extractor.
   */
  public static ExtractorInfo info() {
    return new ExtractorInfo(
        "CSV",
        CsvExtractor.class,
        ExtractorInfo.FEATURE_NONE,
        "Parses the input bytes as a collection of comma-separated-value records.  Returns one Kafka record for each CSV record as defined in RFC4180.  Records are separated by end-of-line characters.");
  }

  /**
   * Constructor.
   *
   * @param config The configuration for the source connector.
   */
  public CsvExtractor(SourceCommonConfig config) {
    super(config, info());
    CSVFormat.Builder builder = CSVFormat.RFC4180.builder();
    if (config.isCsvExtractorHeaderEnabled()) {
      builder.setHeader().setSkipHeaderRecord(true);
    }

    csvFormat = builder.get();
  }

  /**
   * Gets a stream of SchemaAndValue records from the input stream.
   *
   * @param sourceRecord The evolving source regord being processed.
   * @return the stream of values for Kafka SourceRecords.
   */
  @Override
  public Stream<SchemaAndValue> generateRecords(final EvolvingSourceRecord sourceRecord) {
    try (InputStream inputStream = sourceRecord.getInputStream(config.getCompressionType()).get()) {
      return getRecords(IOUtils.toString(inputStream, StandardCharsets.UTF_8)).stream()
          .skip(sourceRecord.getRecordCount())
          .map(this::toConnectData);
    } catch (IOException e) {
      LOGGER.error("Error trying to generate data: {}", e.getMessage(), e);
      return Stream.empty();
    }
  }

  private List<CSVRecord> getRecords(String sourceRecord) {
    try {
      return csvFormat.parse(new StringReader(sourceRecord)).getRecords();
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("IOException occurred when processing csv to CSVRecord %s", e));
    }
  }

  private SchemaBuilder createValueSchema(CSVRecord record) {
    SchemaBuilder valueSchema = SchemaBuilder.struct();
    List<String> headers = record.getParser().getHeaderNames();
    LOGGER.info("headers: {} records: {}", headers, record);

    int limit = Math.max(headers.size(), record.size());
    for (int i = 0; i < limit; i++) {
      if (i < headers.size()) {
        valueSchema.field(headers.get(i), SchemaBuilder.STRING_SCHEMA);
      } else {
        String name = "field" + i;
        valueSchema.field(name, SchemaBuilder.STRING_SCHEMA);
      }
    }
    return valueSchema;
  }

  /**
   * @param value A single line from a csv wrapped in String format with the headers
   * @return A SchemaAndValue object that can be used by Kafka Connect to send Data to Kafka
   */
  private SchemaAndValue toConnectData(CSVRecord value) {
    final Map<String, String> output = new LinkedHashMap<>(value.size());

    SchemaBuilder valueSchema = createValueSchema(value);

    List<Field> fields = valueSchema.fields();
    for (int i = 0; i < fields.size(); i++) {
      // handle the case where there are fewer fields than headers.
      if (i < value.size()) {
        output.put(valueSchema.fields().get(i).name(), value.get(i));
      } else {
        output.put(valueSchema.fields().get(i).name(), "");
      }
    }

    return new SchemaAndValue(valueSchema, output);
  }

  @Override
  public void close() throws Exception {}
}
