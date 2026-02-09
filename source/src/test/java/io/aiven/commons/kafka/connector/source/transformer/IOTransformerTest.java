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

package io.aiven.commons.kafka.connector.source.transformer;

import io.aiven.commons.kafka.connector.source.impl.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeSourceData;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceRecord;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test for transformers.
 */
public abstract class IOTransformerTest {

	/**
	 * The transformer under test.
	 */
	protected Transformer transformer;

	/**
	 * Setup the transformer for testing.
	 * 
	 * @return a configured Transformer.
	 */
	protected abstract Transformer setupTransformer();

	/**
	 * Get the test data in the format for the Transformer.
	 * 
	 * @param numberOfRecords
	 *            the number of recordds in the test data.
	 * @return a byte array containing the data.
	 * @throws IOException
	 *             on error.
	 */
	protected abstract byte[] generateData(int numberOfRecords) throws IOException;

	/**
	 * Get the string prefix for the data messages.
	 * 
	 * @return the string prefix for the data messages.
	 */
	protected abstract String generatedMessagePrefix();

	/**
	 * Given a value object from a SchemaAndValue object extract the message from
	 * it.
	 * 
	 * @return the message to extract.
	 */
	protected abstract Function<Object, String> messageExtractor();

	@BeforeEach
	final void setUp() {
		transformer = setupTransformer();
	}

	@AfterEach
	final void teardown() throws Exception {
		transformer.close();
	}

	@Test
	void testIOExceptionDuringCreation() {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
			@Override
			public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
				return () -> {
					throw new IOException("Exception creating stream");
				};
			}
		};
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).isEmpty();
	}

	@Test
	void testIOExceptionDuringDataRead() throws IOException {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
			@Override
			public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
				return () -> new InputStream() {
					@Override
					public int read() throws IOException {
						throw new IOException("Exception reading data stream");
					}
				};
			}
		};
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);
		assertThat(records).isEmpty();
	}
}
