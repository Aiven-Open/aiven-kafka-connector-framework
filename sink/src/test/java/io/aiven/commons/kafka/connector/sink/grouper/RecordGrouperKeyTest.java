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
package io.aiven.commons.kafka.connector.sink.grouper;

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.kafka.connector.common.templating.TimestampParser;
import io.aiven.commons.kafka.connector.sink.TestingHeader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class RecordGrouperKeyTest {
  private static int ORIGINAL_PARTITION = 2;
  private static int PARTITION = 4;
  private static long ORIGINAL_OFFSET = 32;
  private static long OFFSET = 64;
  private static long TIMESTAMP = 1786716781355L;

  private static Schema valueSchema =
      new SchemaBuilder(Schema.Type.STRUCT)
          .field("string", Schema.STRING_SCHEMA)
          .field("int", Schema.INT32_SCHEMA)
          .field("optString", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  @Test
  void keyTest() {
    Date date = new Date(TIMESTAMP);
    SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            PARTITION,
            Schema.STRING_SCHEMA,
            "key",
            valueSchema,
            Map.of("string", "String value", "int", 6),
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME,
            List.of(
                new TestingHeader("stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new TestingHeader("longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET);
    RecordGrouperKey underTest = new RecordGrouperKey("{{key}}");
    String key = underTest.createKey(sinkRecord);
    assertThat(key).isEqualTo("key");

    underTest = new RecordGrouperKey("{{key}}-{{topic}}-{{partition}}-{{timestamp:unit=yyyy}}");
    key = underTest.createKey(sinkRecord);
    String tsValue = new SimpleDateFormat("yyyy").format(date);
    assertThat(key).isEqualTo("key-topic-4-" + tsValue);

    tsValue = TimestampParser.getFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS").format(date);
    underTest =
        new RecordGrouperKey(
            "{{key}}-{{topic}}-{{partition}}-{{timestamp:unit=yyyy-MM-dd'T'HH:mm:ss.SSS}}");
    key = underTest.createKey(sinkRecord);
    assertThat(key).isEqualTo("key-topic-4-" + tsValue);
  }

  /*
             case "BASIC_ISO_DATE" :
             return DateTimeFormatter.BASIC_ISO_DATE;
         case "ISO_LOCAL_DATE" :
             return DateTimeFormatter.ISO_LOCAL_DATE;
         case "ISO_OFFSET_DATE" :
             return DateTimeFormatter.ISO_OFFSET_DATE;
         case "ISO_DATE" :
             return  DateTimeFormatter.ISO_DATE;
         case "ISO_LOCAL_TIME" :
             return DateTimeFormatter.ISO_LOCAL_TIME;
         case "ISO_OFFSET_TIME" :
             return DateTimeFormatter.ISO_OFFSET_TIME;
         case "ISO_TIME" :
             return DateTimeFormatter.ISO_TIME;
         case "ISO_LOCAL_DATE_TIME" :
             return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
         case "ISO_OFFSET_DATE_TIME" :
             return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
         case "ISO_ZONED_DATE_TIME" :
             return DateTimeFormatter.ISO_ZONED_DATE_TIME;
         case "ISO_DATE_TIME" :
             return DateTimeFormatter.ISO_DATE_TIME;
         case "ISO_ORDINAL_DATE" :
             return DateTimeFormatter.ISO_ORDINAL_DATE;
         case "ISO_WEEK_DATE" :
             return DateTimeFormatter.ISO_WEEK_DATE;
         case "ISO_INSTANT" :
             return DateTimeFormatter.ISO_INSTANT;
         case "RFC_1123_DATE_TIME" :
             return DateTimeFormatter.RFC_1123_DATE_TIME;

  */
}
