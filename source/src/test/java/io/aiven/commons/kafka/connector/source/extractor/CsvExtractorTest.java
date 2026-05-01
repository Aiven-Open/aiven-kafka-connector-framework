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

package io.aiven.commons.kafka.connector.source.extractor;

import static io.aiven.commons.kafka.connector.source.testFixture.format.CsvTestDataFixture.MSG_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.impl.nativeProvided.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.kafka.connector.source.testFixture.format.CsvTestDataFixture;
import io.aiven.commons.util.io.compression.CompressionType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

final class CsvExtractorTest extends IORecordExtractorTest {

  private CsvExtractor extractor;

  @Override
  protected CsvExtractor setupExtractor(CompressionType compressionType) {
    Map<String, String> props = new HashMap<>();
    ConnectorCommonConfigFragment.setter(props).compressionType(compressionType);
    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    return new CsvExtractor(sourceCommonConfig);
  }

  @Override
  protected byte[] generateOneBuffer() throws IOException {
    return generateData(1);
  }

  /**
   * Get the string prefix for the data messages.
   *
   * @return the string prefix for the data messages.
   */
  @Override
  protected String generatedMessagePrefix() {
    return CsvTestDataFixture.MESSAGE_PREFIX;
  }

  /**
   * Get the test data in the format for the Extractor.
   *
   * @param numberOfRecords the number of records in the test data.
   * @return a byte array containing the data.
   * @throws IOException on error.
   */
  @Override
  protected byte[] generateData(int numberOfRecords) throws IOException {
    return CsvTestDataFixture.generateCsvRecords(numberOfRecords).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Given a value object from a SchemaAndValue object extract the message from it.
   *
   * @return the message to extract.
   */
  @Override
  protected Function<Object, String> messageExtractor() {
    return sv -> ((Map) sv).get("value").toString();
  }

  private EvolvingSourceRecord createEvolvingSourceRecord(String nativeItem) {
    final ExampleSourceNativeInfo exp =
        new ExampleSourceNativeInfo(
            new ExampleNativeItem(nativeItem, nativeItem.getBytes(StandardCharsets.UTF_8)));
    return new EvolvingSourceRecord(
        exp, new ExampleOffsetManagerEntry(nativeItem, "group1"), new Context(nativeItem));
  }

  @Test
  void noHeaderTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeadersEnabled(false);
    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem = CsvTestDataFixture.generateCsvRecord(1, "hi");
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);
    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(1);
    Schema schema = records.get(0).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(3);
    assertThat(fields.get(0).name()).isEqualTo("field0");
    assertThat(fields.get(1).name()).isEqualTo("field1");
    assertThat(fields.get(2).name()).isEqualTo("field2");

    Map<String, String> values = (Map) records.get(0).value();
    assertThat(values.get("field0")).isEqualTo("1");
    assertThat(values.get("field1")).isEqualTo("hi");
    assertThat(values.get("field2")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "1");
  }

  @Test
  void shortRowTest() {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeadersEnabled(false);
    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem = CsvTestDataFixture.generateCsvRecord(1, "hi") + "\n2,bye";
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(2);
    Schema schema = records.get(0).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(3);
    assertThat(fields.get(0).name()).isEqualTo("field0");
    assertThat(fields.get(1).name()).isEqualTo("field1");
    assertThat(fields.get(2).name()).isEqualTo("field2");
    Map<String, String> values = (Map) records.get(0).value();
    assertThat(values.get("field0")).isEqualTo("1");
    assertThat(values.get("field1")).isEqualTo("hi");
    assertThat(values.get("field2")).isEqualTo("Hello, from CSV Test Data Fixture: 1");
    schema = records.get(1).schema();
    fields = schema.fields();
    assertThat(fields).hasSize(2);
    assertThat(fields.get(0).name()).isEqualTo("field0");
    assertThat(fields.get(1).name()).isEqualTo("field1");

    values = (Map) records.get(1).value();
    assertThat(values.get("field0")).isEqualTo("2");
    assertThat(values.get("field1")).isEqualTo("bye");
  }

  @Test
  void longRowTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeadersEnabled(false);
    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem =
        CsvTestDataFixture.generateCsvRecord(1, "hi")
            + "\n"
            + CsvTestDataFixture.generateCsvRecord(2, "bye")
            + ",more data";
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(2);
    Schema schema = records.get(1).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("field0");
    assertThat(fields.get(1).name()).isEqualTo("field1");
    assertThat(fields.get(2).name()).isEqualTo("field2");
    assertThat(fields.get(3).name()).isEqualTo("field3");

    Map<String, String> values = (Map) records.get(1).value();
    assertThat(values.get("field0")).isEqualTo("2");
    assertThat(values.get("field1")).isEqualTo("bye");
    assertThat(values.get("field2")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "2");
    assertThat(values.get("field3")).isEqualTo("more data");
  }

  @Test
  void longRowWithHeadersTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeadersEnabled(true);
    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem =
        MSG_HEADER
            + "\n"
            + CsvTestDataFixture.generateCsvRecord(1, "hi")
            + "\n"
            + CsvTestDataFixture.generateCsvRecord(2, "bye")
            + ",more data";
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(2);
    Schema schema = records.get(1).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("id");
    assertThat(fields.get(1).name()).isEqualTo("message");
    assertThat(fields.get(2).name()).isEqualTo("value");
    assertThat(fields.get(3).name()).isEqualTo("field3");

    Map<String, String> values = (Map) records.get(1).value();
    assertThat(values.get("id")).isEqualTo("2");
    assertThat(values.get("message")).isEqualTo("bye");
    assertThat(values.get("value")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "2");
    assertThat(values.get("field3")).isEqualTo("more data");
  }

  @Test
  void tooManyHeadersTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeaders("one, two, three, four");

    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem = CsvTestDataFixture.generateCsvRecords(1);
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(1);
    Schema schema = records.get(0).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("one");
    assertThat(fields.get(1).name()).isEqualTo("two");
    assertThat(fields.get(2).name()).isEqualTo("three");
    assertThat(fields.get(3).name()).isEqualTo("four");

    Map<String, String> values = (Map) records.get(0).value();
    assertThat(values.get("one")).isEqualTo("0");
    assertThat(values.get("two")).isEqualTo(CsvTestDataFixture.TEST_MESSAGE);
    assertThat(values.get("three")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "0");
    assertThat(values.get("four")).isEqualTo("");
  }

  @Test
  void tooFewHeadersTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeaders("one, two");

    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem =
        CsvTestDataFixture.generateCsvRecords(1)
            + CsvTestDataFixture.generateCsvRecord(2, "bye")
            + ",more data";
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(2);
    Schema schema = records.get(0).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(3);
    assertThat(fields.get(0).name()).isEqualTo("one");
    assertThat(fields.get(1).name()).isEqualTo("two");
    assertThat(fields.get(2).name()).isEqualTo("value");

    schema = records.get(1).schema();
    fields = schema.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("one");
    assertThat(fields.get(1).name()).isEqualTo("two");
    assertThat(fields.get(2).name()).isEqualTo("value");
    assertThat(fields.get(3).name()).isEqualTo("field3");

    Map<String, String> values = (Map) records.get(1).value();
    assertThat(values.get("one")).isEqualTo("2");
    assertThat(values.get("two")).isEqualTo("bye");
    assertThat(values.get("value")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "2");
    assertThat(values.get("field3")).isEqualTo("more data");
  }

  @Test
  void tooManyHeadersNonParsedTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props)
        .csvExtractorHeaders("one, two, three, four")
        .csvExtractorHeadersEnabled(false);

    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem =
        CsvTestDataFixture.generateCsvRecord(0, CsvTestDataFixture.TEST_MESSAGE);
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(1);
    Schema schema = records.get(0).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("one");
    assertThat(fields.get(1).name()).isEqualTo("two");
    assertThat(fields.get(2).name()).isEqualTo("three");
    assertThat(fields.get(3).name()).isEqualTo("four");

    Map<String, String> values = (Map) records.get(0).value();
    assertThat(values.get("one")).isEqualTo("0");
    assertThat(values.get("two")).isEqualTo(CsvTestDataFixture.TEST_MESSAGE);
    assertThat(values.get("three")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "0");
    assertThat(values.get("four")).isEqualTo("");
  }

  @Test
  void tooFewHeadersNoneParsedTest() throws Exception {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props)
        .csvExtractorHeaders("one, two")
        .csvExtractorHeadersEnabled(false);

    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem =
        CsvTestDataFixture.generateCsvRecord(0, CsvTestDataFixture.TEST_MESSAGE)
            + "\n"
            + CsvTestDataFixture.generateCsvRecord(2, "bye")
            + ",more data";
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(2);
    Schema schema = records.get(0).schema();
    List<Field> fields = schema.fields();
    assertThat(fields).hasSize(3);
    assertThat(fields.get(0).name()).isEqualTo("one");
    assertThat(fields.get(1).name()).isEqualTo("two");
    assertThat(fields.get(2).name()).isEqualTo("field2");

    schema = records.get(1).schema();
    fields = schema.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("one");
    assertThat(fields.get(1).name()).isEqualTo("two");
    assertThat(fields.get(2).name()).isEqualTo("field2");
    assertThat(fields.get(3).name()).isEqualTo("field3");

    Map<String, String> values = (Map) records.get(1).value();
    assertThat(values.get("one")).isEqualTo("2");
    assertThat(values.get("two")).isEqualTo("bye");
    assertThat(values.get("field2")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "2");
    assertThat(values.get("field3")).isEqualTo("more data");
  }

  @Test
  void multipleCSVFilesParsedTest() {
    Map<String, String> props = new HashMap<>();
    props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItemOne =
        CsvTestDataFixture.generateCsvRecords(0, 1, CsvTestDataFixture.TEST_MESSAGE, MSG_HEADER);
    final String nativeItemTwo =
        CsvTestDataFixture.generateCsvRecords(
            0,
            1,
            CsvTestDataFixture.TEST_MESSAGE,
            CSVFormat.RFC4180.format("message", "id", "value"));

    final EvolvingSourceRecord sourceRecordOne = createEvolvingSourceRecord(nativeItemOne);
    final EvolvingSourceRecord sourceRecordTwo = createEvolvingSourceRecord(nativeItemTwo);
    final List<SchemaAndValue> batchOne = extractor.generateRecords(sourceRecordOne).toList();
    final List<SchemaAndValue> batchTwo = extractor.generateRecords(sourceRecordTwo).toList();

    assertThat(batchOne).hasSize(1);
    Schema schema = batchOne.get(0).schema();
    assertEquals("id", schema.fields().get(0).name());
    assertEquals("message", schema.fields().get(1).name());
    assertEquals("value", schema.fields().get(2).name());
    assertThat(batchTwo).hasSize(1);
    schema = batchTwo.get(0).schema();
    assertEquals("message", schema.fields().get(0).name());
    assertEquals("id", schema.fields().get(1).name());
    assertEquals("value", schema.fields().get(2).name());
  }

  @Test
  void readJsonDataFromConverter() throws IOException {
    Map<String, String> props = new HashMap<>();
    SourceConfigFragment.setter(props).csvExtractorHeadersEnabled(false);
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    SourceCommonConfig sourceCommonConfig =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    extractor = new CsvExtractor(sourceCommonConfig);
    final String nativeItem =
        CsvTestDataFixture.generateCsvRecord(1, "hi")
            + "\n"
            + CsvTestDataFixture.generateCsvRecord(2, "bye")
            + ",more data";
    final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

    final List<SchemaAndValue> records = extractor.generateRecords(sourceRecord).toList();
    assertThat(records.size()).isEqualTo(2);
    Schema schema = records.get(0).schema();
    JsonConverter converter = new JsonConverter();
    // We are converting the value portion of the kafka event
    converter.configure(Map.of("converter.type", "value"));
    var result = converter.fromConnectData(null, schema, records.get(0).value());
    ObjectMapper mapper = new ObjectMapper();
    assertThat(result).isNotNull();
    Map<String, Map<String, String>> mapResult = mapper.readValue(result, Map.class);
    Map<String, String> payload = mapResult.get("payload");
    assertThat(payload).isNotNull();
    assertThat(payload.containsKey("field0")).isTrue();
    assertThat(payload.containsKey("field1")).isTrue();
    assertThat(payload.containsKey("field2")).isTrue();
    assertThat(payload.size()).isEqualTo(3);
    var resultTwo =
        converter.fromConnectData(null, records.get(1).schema(), records.get(1).value());
    assertThat(result).isNotNull();
    Map<String, Map<String, String>> mapResultTwo = mapper.readValue(resultTwo, Map.class);
    Map<String, String> payloadTwo = mapResultTwo.get("payload");
    assertThat(payloadTwo.size()).isEqualTo(4);
    assertThat(payloadTwo.get("field0")).isEqualTo("2");
    assertThat(payloadTwo.get("field1")).isEqualTo("bye");
    assertThat(payloadTwo.get("field2")).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + "2");
    assertThat(payloadTwo.get("field3")).isEqualTo("more data");
  }
}
