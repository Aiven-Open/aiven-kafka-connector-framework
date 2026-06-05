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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** CSV Test data fixture */
public class CsvTestDataFixture {
  /** Default message prefix for generated messages */
  public static final String MESSAGE_PREFIX = "Hello, from CSV Test Data Fixture: ";

  /** Default CSV headers. */
  public static final String MSG_HEADER = CSVFormat.RFC4180.format("id", "message", "value");

  /** Default test message */
  public static final String TEST_MESSAGE = "test message";

  private CsvTestDataFixture() {
    // do not instantiate
  }

  /**
   * Generates a byte array containing the specified number of records. Generates {@code numRecs}
   * records with IDs in the range @{code [0..numRecs)}, with {@link #TEST_MESSAGE} as the message
   * text.
   *
   * @param numRecs the numer of records to generate
   * @return A byte array containing the specified number of records.
   */
  public static byte[] generateCsvData(final int numRecs) {
    return generateCsvData(0, numRecs);
  }

  /**
   * Generates a byte array containing the specified number of records. Generates {@code numRecs}
   * records with IDs in the range @{code [messageId..messageId+numRecs)}, with {@link
   * #TEST_MESSAGE} as the message text.
   *
   * @param messageId the messageId to start with.
   * @param numOfRecs the number of records to write.
   * @return A byte array containing the specified number of records.
   */
  public static byte[] generateCsvData(final int messageId, final int numOfRecs) {
    return generateCsvRecords(messageId, numOfRecs, TEST_MESSAGE).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * /** Generates and serializes the specified number of records. Generates {@code numRecs} records
   * with IDs in the range @{code [0..numRecs)}, with {@link #TEST_MESSAGE} as the message text.
   *
   * @param numRecs the number of records to generate.
   * @return The specified number of CSV records encoded into a string.
   */
  public static String generateCsvRecords(final int numRecs) {
    return generateCsvRecords(0, numRecs, TEST_MESSAGE);
  }

  /**
   * Generates a single CSV record with the format {@code messageId,msg,MESSAGE_PREFIX+messageId}.
   *
   * @param messageId the id for the record
   * @param msg the message for the record
   * @return a standard CSV test record.
   */
  public static String generateCsvRecord(final int messageId, final String msg) {
    return CSVFormat.RFC4180.format(messageId, msg, MESSAGE_PREFIX + messageId);
  }

  /**
   * Generates a multiple CSV records with the format {@code
   * messageId,msg,MESSAGE_PREFIX+messageId}. Generates {@code numRecs} records with IDs in the
   * range @{code [messageId..messageId+numRecs)}, with Records are concatenated into a string with
   * each record separated by {@code \n}. The initial entry comprises the headers as specified in
   * {@link #MSG_HEADER}
   *
   * @param messageId the message ID for the record.
   * @param numRecs the number of records to create.
   * @param testMessage the message for the records.
   * @return the string representing the csv records.
   */
  public static String generateCsvRecords(
      final int messageId, final int numRecs, final String testMessage) {
    return generateCsvRecords(messageId, numRecs, testMessage, MSG_HEADER);
  }

  /**
   * Generates a multiple CSV records with the format {@code
   * messageId,msg,MESSAGE_PREFIX+messageId}. Generates {@code numRecs} records with IDs in the
   * range @{code [messageId..messageId+numRecs)}, with Records are concatenated into a string with
   * each record separated by {@code \n}. The initial entry comprises the specified headers.
   *
   * @param messageId the message ID for the record.
   * @param recordCount the number of records to create.
   * @param testMessage the message for the records.
   * @param headers the headers for the csv columns.
   * @return the string representing the csv records.
   */
  public static String generateCsvRecords(
      final int messageId, final int recordCount, final String testMessage, String headers) {
    final StringBuilder csvRecords = new StringBuilder(headers).append(System.lineSeparator());
    for (int i = 0; i < recordCount; i++) {
      csvRecords.append(generateCsvRecord(messageId + i, testMessage)).append("\n");
    }
    return csvRecords.toString();
  }

  /**
   * Reads a CSV record from the byte array.
   *
   * @param bytes the bytes to extract the record from.
   * @return CSVRecord read from the bytes.
   * @throws IOException on IO error.
   */
  public static CSVRecord readCsvRecord(final byte[] bytes) throws IOException {
    return CSVParser.parse(
            new ByteArrayInputStream(bytes), StandardCharsets.UTF_8, CSVFormat.RFC4180)
        .getRecords()
        .get(0);
  }

  /**
   * Reads multiple CSV records.
   *
   * @param values The Strings containing the serialized CSV records.
   * @return a list of CsvRecords extracted from the values.
   * @throws IOException on IO error.
   */
  public static List<CSVRecord> readCsvRecords(final Collection<String> values) throws IOException {
    final List<CSVRecord> result = new ArrayList<>();
    for (final String value : values) {
      result.addAll(CSVParser.parse(new StringReader(value), CSVFormat.RFC4180).getRecords());
    }
    return result;
  }

  /**
   * Reads a list of CsvRecords from an array of bytes. Reads the bytes line by line.
   *
   * @param bytes the serialized CSV records.
   * @return a list of {@code CSVRecord}s extracted from the values.
   * @throws IOException on IO error.
   */
  public static List<CSVRecord> readCsvRecords(final byte[] bytes) throws IOException {
    return CSVFormat.RFC4180
        .builder()
        .setHeader()
        .setSkipHeaderRecord(true)
        .get()
        .parse(new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8))
        .getRecords();
  }
}
