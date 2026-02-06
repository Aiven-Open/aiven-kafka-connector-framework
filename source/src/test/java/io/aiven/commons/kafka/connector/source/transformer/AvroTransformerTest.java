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

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeSourceData;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceRecord;
import io.aiven.commons.kafka.connector.source.testFixture.format.AvroTestDataFixture;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

final class AvroTransformerTest {

	private AvroTransformer avroTransformer;

	@BeforeEach
	void setUp() {
		Map<String, String> props = new HashMap<>();
		SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
		setter.transformerCache(100);
		SourceCommonConfig sourceCommonConfig = new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(),
				props);
		avroTransformer = new AvroTransformer(sourceCommonConfig);
	}

	@Test
	void testReadAvroRecordsInvalidData() {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				ByteBuffer.wrap("mock-avro-data".getBytes(StandardCharsets.UTF_8)));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Stream<SchemaAndValue> records = avroTransformer.generateRecords(nativeSourceData, sourceRecord);
		final List<Object> recs = records.collect(Collectors.toList());
		assertThat(recs).isEmpty();
	}

	@Test
	void testReadAvroRecords() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				AvroTestDataFixture.generateAvroData(25));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final List<String> expected = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			expected.add("Hello, from Avro Test Data Fixture! object " + i);
		}

		final Stream<SchemaAndValue> records = avroTransformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).extracting(SchemaAndValue::value).extracting(sv -> ((Struct) sv).getString("message"))
				.containsExactlyElementsOf(expected);
	}

	@Test
	void testReadAvroRecordsSkipFew() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				AvroTestDataFixture.generateAvroData(20));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
		// skip 5 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(5);
		sourceRecord.setOffsetManagerEntry(entry);

		final List<String> expected = new ArrayList<>();
		for (int i = 5; i < 20; i++) {
			expected.add("Hello, from Avro Test Data Fixture! object " + i);
		}
		final Stream<SchemaAndValue> records = avroTransformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).extracting(SchemaAndValue::value).extracting(sv -> ((Struct) sv).getString("message"))
				.containsExactlyElementsOf(expected);
	}

	@Test
	void testReadAvroRecordsSkipMoreRecordsThanExist() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				AvroTestDataFixture.generateAvroData(20));
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
		// skip 25 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(25);
		sourceRecord.setOffsetManagerEntry(entry);

		final Stream<SchemaAndValue> records = avroTransformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).isEmpty();
	}

}
