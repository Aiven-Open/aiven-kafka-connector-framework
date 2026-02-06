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

 package io.aiven.commons.kafka.connector.source.transformer;


 import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
 import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
 import io.aiven.commons.kafka.connector.source.impl.ExampleNativeItem;
 import io.aiven.commons.kafka.connector.source.impl.ExampleNativeSourceData;
 import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
 import io.aiven.commons.kafka.connector.source.impl.ExampleSourceRecord;
 import io.aiven.commons.kafka.connector.source.testFixture.format.JsonTestDataFixture;
 import org.apache.commons.io.function.IOSupplier;
 import org.apache.kafka.connect.data.SchemaAndValue;
 import org.junit.jupiter.api.BeforeEach;
 import org.junit.jupiter.api.Test;


 import java.io.ByteArrayInputStream;
 import java.io.IOException;
 import java.io.InputStream;
 import java.nio.ByteBuffer;
 import java.nio.charset.StandardCharsets;
 import java.util.ArrayList;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.util.stream.Collectors;
 import java.util.stream.Stream;

 import static org.assertj.core.api.Assertions.assertThat;


 final class JsonTransformerTest {

  private JsonTransformer jsonTransformer;

  @BeforeEach
  void setUp() {
   Map<String, String> props = new HashMap<>();
   SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
   setter.transformerCache(100);
   SourceCommonConfig sourceCommonConfig = new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
   jsonTransformer = new JsonTransformer(sourceCommonConfig);
  }

  @Test
  void testHandleValueDataWithInvalidJson() throws IOException {
   final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", ByteBuffer.wrap("mock-json-data".getBytes(StandardCharsets.UTF_8)));
   final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
   final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

   final Stream<SchemaAndValue> records = jsonTransformer.generateRecords(nativeSourceData, sourceRecord);
   final List<Object> recs = records.collect(Collectors.toList());
   assertThat(recs).isEmpty();

  }

  @Test
  void testHandleValueDataWithValidJson() throws IOException {
   final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", JsonTestDataFixture.generateJsonData(100));
   final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
   final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);


   final Stream<SchemaAndValue> records = jsonTransformer.generateRecords(nativeSourceData, sourceRecord);

   final List<String> expected = new ArrayList<>();
   for (int i = 0; i < 100; i++) {
    expected.add("value" + i);
   }


   assertThat(records).extracting(SchemaAndValue::value).extracting(sv -> ((Map) sv).get("key"))
           .containsExactlyElementsOf(expected);
  }

  @Test
  void testHandleValueDataWithValidJsonSkipFew() throws IOException {

   final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", JsonTestDataFixture.generateJsonData(20));
   final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
   final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
   // skip 5 records -- we have to set the record after the read because the getOffsetManagerEntry() creates a defensive copy
   final ExampleOffsetManagerEntry entry = sourceRecord.getOffsetManagerEntry();
   entry.setRecordCount(5);
   sourceRecord.setOffsetManagerEntry(entry);

   final List<String> expected = new ArrayList<>();
   for (int i = 5; i < 20; i++) {
    expected.add("Test message" + i);
   }
   final Stream<SchemaAndValue> records = jsonTransformer.generateRecords(nativeSourceData, sourceRecord);

   assertThat(records).extracting(SchemaAndValue::value).extracting(sv -> ((Map) sv).get("key"))
           .containsExactlyElementsOf(expected);

  }

  @Test
  void testReadAvroRecordsSkipMoreRecordsThanExist() throws Exception {
   final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", JsonTestDataFixture.generateJsonData(20));
   final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
   final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

   // skip 25 records -- we have to set the record after the read because the getOffsetManagerEntry() creates a defensive copy
   final ExampleOffsetManagerEntry entry = sourceRecord.getOffsetManagerEntry();
   entry.setRecordCount(25);
   sourceRecord.setOffsetManagerEntry(entry);

   final Stream<SchemaAndValue> records = jsonTransformer.generateRecords(nativeSourceData, sourceRecord);

   assertThat(records).isEmpty();
  }

  @Test
  void testGetRecordsWithIOException() throws IOException {
   final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", JsonTestDataFixture.generateJsonData(20));

   final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
    @Override
    public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
     return () -> new InputStream() {
      @Override
      public int read() throws IOException {
       throw new IOException("Test Exception");
      }
     };
    }
   };
   final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

   final Stream<SchemaAndValue> records = jsonTransformer.generateRecords(nativeSourceData, sourceRecord);

   assertThat(records).isEmpty();
  }

  @Test
  void testCustomSpliteratorWithIOExceptionDuringInitialization() throws
          IOException {
   final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", JsonTestDataFixture.generateJsonData(20));
   final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
    public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
     return new IOSupplier<InputStream>() {
      @Override
      public InputStream get() throws IOException {
       throw new IOException("Test IOException during initialization");
      }
     };
    }
   };

   final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
   final Stream<SchemaAndValue> records = jsonTransformer.generateRecords(nativeSourceData, sourceRecord);

   assertThat(records).isEmpty();
  }

 }
