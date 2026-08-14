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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class RecordGrouperTest {

  @Test
  void keyPartitioning() {
    int ORIGINAL_PARTITION = 2;
    int PARTITION = 4;
    long ORIGINAL_OFFSET = 32;
    // 2026-08-14T14:13:01
    long TIMESTAMP = 1786716781355L;
    long OFFSET = 64;

    RecordGrouperKey key =
        new RecordGrouperKey(
            "{{key}}-{{topic}}-{{partition}}-{{timestamp:unit=yyyy-MM-dd'T'HH:mm:ss}}");
    RecordGrouperImpl underTest = new RecordGrouperImpl(key);

    underTest.put(
        new SinkRecord(
            "topic",
            PARTITION,
            Schema.STRING_SCHEMA,
            "key1",
            Schema.STRING_SCHEMA,
            "record1",
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME,
            List.of(
                new RecordGrouperKeyTest.TestingHeader(
                    "stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new RecordGrouperKeyTest.TestingHeader(
                    "longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET));

    underTest.put(
        new SinkRecord(
            "topic",
            PARTITION,
            Schema.STRING_SCHEMA,
            "key1",
            Schema.STRING_SCHEMA,
            "record2",
            OFFSET,
            TIMESTAMP,
            TimestampType.CREATE_TIME,
            List.of(
                new RecordGrouperKeyTest.TestingHeader(
                    "stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new RecordGrouperKeyTest.TestingHeader(
                    "longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET));

    underTest.put(
        new SinkRecord(
            "topic",
            PARTITION,
            Schema.STRING_SCHEMA,
            "key1",
            Schema.STRING_SCHEMA,
            "record3",
            OFFSET,
            TIMESTAMP + 1000,
            TimestampType.CREATE_TIME,
            List.of(
                new RecordGrouperKeyTest.TestingHeader(
                    "stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new RecordGrouperKeyTest.TestingHeader(
                    "longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET));

    underTest.put(
        new SinkRecord(
            "topic2",
            PARTITION,
            Schema.STRING_SCHEMA,
            "key1",
            Schema.STRING_SCHEMA,
            "record4",
            OFFSET,
            TIMESTAMP + 500,
            TimestampType.CREATE_TIME,
            List.of(
                new RecordGrouperKeyTest.TestingHeader(
                    "stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new RecordGrouperKeyTest.TestingHeader(
                    "longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET));

    underTest.put(
        new SinkRecord(
            "topic2",
            PARTITION,
            Schema.STRING_SCHEMA,
            "key0",
            Schema.STRING_SCHEMA,
            "record4",
            OFFSET,
            TIMESTAMP + 1000,
            TimestampType.CREATE_TIME,
            List.of(
                new RecordGrouperKeyTest.TestingHeader(
                    "stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new RecordGrouperKeyTest.TestingHeader(
                    "longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET));

    underTest.put(
        new SinkRecord(
            "topic2",
            PARTITION + 1,
            Schema.STRING_SCHEMA,
            "key0",
            Schema.STRING_SCHEMA,
            "record4",
            OFFSET,
            TIMESTAMP + 500,
            TimestampType.CREATE_TIME,
            List.of(
                new RecordGrouperKeyTest.TestingHeader(
                    "stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new RecordGrouperKeyTest.TestingHeader(
                    "longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42))),
            "originalTopic",
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET));

    assertFalse(underTest.storage.isEmpty());
    assertEquals(5, underTest.storage.size());
    for (String testKey : underTest.storage.keySet()) {
      if (testKey.equals("key1-topic-4-2026-08-14T14:13:01")) {
        assertEquals(2, underTest.storage.get(testKey).size());
      } else {
        assertEquals(1, underTest.storage.get(testKey).size());
      }
    }
  }

  private static class RecordGrouperImpl extends RecordGrouper {
    Map<String, List<SinkRecord>> storage = new TreeMap<>();

    /**
     * Creates the RecordGroup using the specified RecordGrouperKey.
     *
     * @param grouperKey the RecordGrouperKey to use to group records.
     */
    protected RecordGrouperImpl(RecordGrouperKey grouperKey) {
      super(grouperKey);
    }

    @Override
    protected String put(String key, SinkRecord record) {
      storage.compute(
          key,
          (k, v) -> {
            if (v == null) {
              v = new ArrayList<>();
            }
            v.add(record);
            return v;
          });
      return key;
    }
  }
}
