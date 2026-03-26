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

import java.util.Arrays;

/** The fixture to manipulate byte array data for tests. */
public class ByteArrayDataFixture {

  /**
   * Generates a byte array containing the specified number of bytes.
   *
   * @param numBytes the numer of bytes to generate
   * @return A byte array containing the specified number of bytes.
   */
  public static byte[] generateByteData(final int numBytes) {
    return generateByteData(numBytes, 0);
  }

  /**
   * Converts an int into a byte using the modulus of Byte.MAX_VALUE
   *
   * @param value the value to convert.
   * @return a byte.
   */
  public static byte intAsByte(int value) {
    return (byte) (value % Byte.MAX_VALUE);
  }

  /**
   * Generates a buffer with the requested number of bytes where the {@link #intAsByte} conversion
   * starts at the specified offset.
   *
   * @param numBytes the number of bytes in the final buffer.
   * @param offset the offset from which to start counting.
   * @return the specified buffer.
   */
  public static byte[] generateByteData(final int numBytes, int offset) {
    byte[] result = new byte[numBytes];
    for (int i = 0; i < numBytes; i++) {
      result[i] = intAsByte(i + offset);
    }
    return result;
  }

  /**
   * Generate a number of records. Each record is filled the {@link #intAsByte} value for the record
   * number.
   *
   * @param bufferSize the buffer size for each record.
   * @param numberOfRecords the number of records to generate.
   * @return a buffer containing all the records.
   */
  public static byte[] generateByteRecords(final int bufferSize, final int numberOfRecords) {
    byte[] result = new byte[bufferSize * numberOfRecords];
    for (int i = 0; i < numberOfRecords; i++) {
      System.arraycopy(
          generateByteRecord(bufferSize, intAsByte(i)), 0, result, i * bufferSize, bufferSize);
    }
    return result;
  }

  /**
   * Generate a buffer of a specified size containing the fill value.
   *
   * @param bufferSize the buffer size.
   * @param fill the fill byte.
   * @return a byte array filled with the {@code fill} value.
   */
  public static byte[] generateByteRecord(final int bufferSize, final byte fill) {
    byte[] result = new byte[bufferSize];
    Arrays.fill(result, fill);
    return result;
  }
}
