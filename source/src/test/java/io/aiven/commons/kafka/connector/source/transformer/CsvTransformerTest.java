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

import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.kafka.connector.source.testFixture.format.CsvTestDataFixture;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

final class CsvTransformerTest {

	private CsvTransformer transformer;

	private CsvTransformer setupTransformer() {
		Map<String, String> props = new HashMap<>();
		SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
		setter.transformerCache(100);
		SourceCommonConfig sourceCommonConfig = new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(),
				props);
		return new CsvTransformer(sourceCommonConfig);
	}

	/**
	 * Get the string prefix for the data messages.
	 *
	 * @return the string prefix for the data messages.
	 */

	private String generatedMessagePrefix() {
		return CsvTestDataFixture.MESSAGE_PREFIX;
	}

	/**
	 * Get the test data in the format for the Transformer.
	 *
	 * @param numberOfRecords
	 *            the number of records in the test data.
	 * @return a byte array containing the data.
	 * @throws IOException
	 *             on error.
	 */

	private String generateData(int numberOfRecords) throws IOException {
		return CsvTestDataFixture.generateCsvRecords(numberOfRecords);
	}

	/**
	 * Given a value object from a SchemaAndValue object extract the message from
	 * it.
	 *
	 * @return the message to extract.
	 */
	private Function<Object, String> messageExtractor() {
		return sv -> ((Map) sv).get("value").toString();
	}

	@BeforeEach
	void setUp() {
		transformer = setupTransformer();
	}

	@AfterEach
	void teardown() throws Exception {
		transformer.close();
	}

	@Test
	void testReadRecordsInvalidData() throws IOException {
		final String nativeItem = "A-bad-data-block";

		final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);
		final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);
		final List<Object> recs = records.collect(Collectors.toList());
		assertThat(recs).isEmpty();
	}

	@Test
	void testReadData() throws Exception {
		final String nativeItem = generateData(25);

		final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);

		final List<String> expected = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			expected.add(generatedMessagePrefix() + i);
		}

		final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);
		assertThat(records).extracting(SchemaAndValue::value).extracting(messageExtractor())
				.containsExactlyElementsOf(expected);
	}

	@Test
	void testReadRecordsSkipFew() throws Exception {
		final String nativeItem = generateData(20);

		final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);
		// skip 5 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = (ExampleOffsetManagerEntry) sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(5);
		sourceRecord.setOffsetManagerEntry(entry);

		final List<String> expected = new ArrayList<>();
		for (int i = 5; i < 20; i++) {
			expected.add(generatedMessagePrefix() + i);
		}
		final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

		assertThat(records).extracting(SchemaAndValue::value).extracting(messageExtractor())
				.containsExactlyElementsOf(expected);
	}

	private EvolvingSourceRecord createEvolvingSourceRecord(String nativeItem) {
		final ExampleSourceNativeInfo exp = new ExampleSourceNativeInfo(
				new ExampleNativeItem(nativeItem, nativeItem.getBytes(StandardCharsets.UTF_8)));
		return new EvolvingSourceRecord(exp, new ExampleOffsetManagerEntry(nativeItem, "group1"),
				new Context(nativeItem));
	}

	@Test
	void testReadRecordsSkipMoreRecordsThanExist() throws Exception {
		final String nativeItem = generateData(20);

		final EvolvingSourceRecord sourceRecord = createEvolvingSourceRecord(nativeItem);
		// skip 25 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = (ExampleOffsetManagerEntry) sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(25);
		sourceRecord.setOffsetManagerEntry(entry);

		final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

		assertThat(records).isEmpty();

	}
}
