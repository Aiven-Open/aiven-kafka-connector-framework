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
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test for transformers that provide consume {@code IOSource<InputStream>}
 * objects and produce records.
 */
public abstract class IORecordTransformerTest extends IOTransformerTest {

	/**
	 * Get the string prefix for the data messages.
	 * 
	 * @return the string prefix for the data messages.
	 */
	protected abstract String generatedMessagePrefix();

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
	 * Given a value object from a SchemaAndValue object extract the message from
	 * it.
	 * 
	 * @return the message to extract.
	 */
	protected abstract Function<Object, String> messageExtractor();

	/**
	 * Test that invalid data does not throw an exception to the reader and does not
	 * return any daa.
	 */
	@Test
	final void testReadRecordsInvalidData() {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				ByteBuffer.wrap("A-bad-data-block".getBytes(StandardCharsets.UTF_8)));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);
		final List<Object> recs = records.collect(Collectors.toList());
		assertThat(recs).isEmpty();
	}

	@Override
	@Test
	final void testReadData() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateData(25));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final List<String> expected = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			expected.add(generatedMessagePrefix() + i);
		}

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		// List<SchemaAndValue> recordList = records.toList();
		// for( var record : recordList) {
		// var value = record.value();
		// System.out.println(value);
		// }

		assertThat(records).extracting(SchemaAndValue::value).extracting(messageExtractor())
				.containsExactlyElementsOf(expected);
	}

	@Override
	@Test
	final void testReadRecordsSkipFew() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateData(20));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
		// skip 5 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(5);
		sourceRecord.setOffsetManagerEntry(entry);

		final List<String> expected = new ArrayList<>();
		for (int i = 5; i < 20; i++) {
			expected.add(generatedMessagePrefix() + i);
		}
		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).extracting(SchemaAndValue::value).extracting(messageExtractor())
				.containsExactlyElementsOf(expected);
	}

	@Override
	@Test
	final void testReadRecordsSkipMoreRecordsThanExist() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateData(20));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
		// skip 25 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(25);
		sourceRecord.setOffsetManagerEntry(entry);

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).isEmpty();
	}
}
