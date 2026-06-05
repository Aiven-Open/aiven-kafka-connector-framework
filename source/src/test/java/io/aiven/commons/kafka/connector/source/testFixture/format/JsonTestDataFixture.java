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

package io.aiven.commons.kafka.connector.source.testFixture.format;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** A testing fixture to generate/read JSON data. */
public final class JsonTestDataFixture {
  /** Default test message */
  public static final String TEST_MESSAGE = "test message";

  /** Default message prefix */
  public static final String MESSAGE_PREFIX = "Hello, from JSON Test Data Fixture: ";

  /** Default message format */
  private static final String MSG_FORMAT =
      "{\"id\" : %s, \"message\" : \"%s\", \"value\" : \"%s\"}%n";

  /** Default schema string */
  public static final String SCHEMA_JSON =
      "{\n  \"type\": \"struct\", \"name\": \"TestRecord\",\n "
          + "  \"fields\": [\n {\"field\": \"message\", \"type\": \"string\"},\n"
          + "    {\"field\": \"id\", \"type\": \"int32\"}\n  ]\n}";

  /** Schema with extra data elements */
  public static final String CONNECT_EXTRA_SCHEMA_JSON =
      "{\n  \"type\": \"struct\",\n  \"name\": \"TestRecord\",\n"
          + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
          + "    {\"name\": \"id\", \"type\": \"int32\"}\n  ],\n"
          + "    \"connect.version\":1, \"connect.name\": \"TestRecord\"}\n";

  /** The expected evolved schema */
  public static final String EVOLVED_SCHEMA_JSON =
      "{\n  \"type\": \"struct\",\n  \"name\": \"TestRecord\",\n"
          + "  \"fields\": [\n    {\"field\": \"message\", \"type\": \"string\"},\n"
          + "    {\"field\": \"id\", \"type\": \"int32\"},\n"
          + "    {\"field\": \"age\", \"type\": \"int32\", \"default\":0}\n  ]\n}";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final DeserializationFeature[] DESERIALIZATION_FEATURES = {
    DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS
  };

  static {
    for (final DeserializationFeature feature : DESERIALIZATION_FEATURES) {
      OBJECT_MAPPER.enable(feature);
    }
    OBJECT_MAPPER.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
  }

  private JsonTestDataFixture() {
    // do not instantiate
  }

  /**
   * Generates and serializes the specified number of records. Records have IDs in the range @{code
   * [0..numRecs)}, with {@link #TEST_MESSAGE} as the message text.
   *
   * @param numRecs the numer of records to generate
   * @return A byte array containing the specified number of records.
   */
  public static byte[] generateJsonData(final int numRecs) {
    return generateJsonData(0, numRecs);
  }

  /**
   * Generates and serializes the specified number of records. Records have IDs in the range @{code
   * [messageId..messageId+numRecs)}, with {@link #TEST_MESSAGE} as the message text.
   *
   * @param messageId the messageId to start with.
   * @param numRecs the number of records to write.
   * @return A byte array containing the specified number of records.
   */
  @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
  public static byte[] generateJsonData(final int messageId, final int numRecs) {
    return generateJsonRecords(messageId, numRecs, TEST_MESSAGE).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Generates and serializes the specified number of records. Records have IDs in the range @{code
   * [0..numRecs)}, with {@link #TEST_MESSAGE} as the message text. Records have the format
   * specified by {@link #MSG_FORMAT}, and with the value set to {@link #MESSAGE_PREFIX} + {@code
   * messageId}.
   *
   * @param numRecs the number of records to generate.
   * @return the String comprising the concatenated JSON records.
   */
  public static String generateJsonRecords(final int numRecs) {
    return generateJsonRecords(0, numRecs, TEST_MESSAGE);
  }

  /**
   * Generates a single JSON record with the format specified by {@link #MSG_FORMAT}, and with the
   * value set to {@link #MESSAGE_PREFIX} + {@code messageId}.
   *
   * @param messageId the id for the record
   * @param msg the message for the record
   * @return a standard JSON test record.
   */
  public static String generateJsonRecord(final int messageId, final String msg) {
    return String.format(MSG_FORMAT, messageId, msg, MESSAGE_PREFIX + messageId);
  }

  /**
   * Generates and serializes the specified number of records. Records have IDs in the range @{code
   * [messageId..messageId+numRecs)}, with the specified as the message text. Records have the
   * format specified by {@link #MSG_FORMAT}, and with the value set to {@link #MESSAGE_PREFIX} +
   * {@code messageId}.
   *
   * @param messageId the messageId to start with.
   * @param numRecs the number of records to write.
   * @param msg the message for the records.
   * @return the String comprising the concatenated JSON records.
   */
  public static String generateJsonRecords(
      final int messageId, final int numRecs, final String msg) {
    final StringBuilder jsonRecords = new StringBuilder();
    for (int i = 0; i < numRecs; i++) {
      jsonRecords.append(generateJsonRecord(messageId + i, msg));
    }
    return jsonRecords.toString();
  }

  /**
   * Reads a JsonNode from the byte array.
   *
   * @param bytes the bytes to extract the record from.
   * @return JsonNode read from the bytes.
   * @throws IOException on IO error.
   */
  public static JsonNode readJsonRecord(final byte[] bytes) throws IOException {
    return OBJECT_MAPPER.readTree(bytes);
  }

  /**
   * Reads multiple JSON records.
   *
   * @param values The Strings containing the serialized JSON records.
   * @return a list of JsonNodes extracted from the values.
   * @throws IOException on IO error.
   */
  public static List<JsonNode> readJsonRecords(final Collection<String> values) throws IOException {
    final List<JsonNode> result = new ArrayList<>();
    for (final String value : values) {
      result.add(OBJECT_MAPPER.readTree(value));
    }
    return result;
  }

  /**
   * Reads a list of JsonNode from an array of bytes. Reads the bytes line by line.
   *
   * @param bytes the serialized JSON records.
   * @return a list of JsonNodes extracted from the values.
   * @throws IOException on IO error.
   */
  public static List<JsonNode> readJsonRecords(final byte[] bytes) throws IOException {
    final List<JsonNode> result = new ArrayList<>();
    for (final String value : readLines(bytes)) {
      result.add(OBJECT_MAPPER.readTree(value));
    }
    return result;
  }

  /**
   * Reads based lines from the byte array.
   *
   * @param input the serialized data
   * @return a list of lines from the byte data.
   * @throws IOException on IO error.
   */
  public static List<String> readLines(final byte[] input) throws IOException {
    try (InputStreamReader reader =
            new InputStreamReader(new ByteArrayInputStream(input), StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(reader)) {
      return bufferedReader.lines().collect(Collectors.toList());
    }
  }
}
