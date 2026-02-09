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

import java.io.IOException;
import java.util.Arrays;

public class ByteArrayDataFixture {

	/**
	 * Generates a byte array containing the specified number of records.
	 *
	 * @param numBytes
	 *            the numer of records to generate
	 * @return A byte array containing the specified number of records.
	 * @throws IOException
	 *             if the Avro records can not be serialized.
	 */
	public static byte[] generateByteData(final int numBytes) {
		return generateByteData(numBytes, 0);
	}

	public static byte intAsByte(int value) {
		return (byte) (value % Byte.MAX_VALUE);
	}

	public static byte[] generateByteData(final int numBytes, int offset) {
		byte[] result = new byte[numBytes];
		for (int i = 0; i < numBytes; i++) {
			result[i] = intAsByte(i + offset);
		}
		return result;
	}

	public static byte[] generateByteRecords(final int bufferSize, final int numberOfRecords) {
		byte[] result = new byte[bufferSize * numberOfRecords];
		for (int i = 0; i < numberOfRecords; i++) {
			byte[] src = generateByteRecord(bufferSize, intAsByte(i));
			System.arraycopy(generateByteRecord(bufferSize, intAsByte(i)), 0, result, i * bufferSize, bufferSize);
		}
		return result;
	}

	public static byte[] generateByteRecord(final int bufferSize, final byte fill) {
		byte[] result = new byte[bufferSize];
		Arrays.fill(result, fill);
		return result;
	}
}
