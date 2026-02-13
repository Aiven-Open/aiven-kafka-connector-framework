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
import io.aiven.commons.kafka.connector.source.testFixture.format.ByteArrayDataFixture;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Byte array transformer test.
 */
final class ByteArrayTransformerTest extends IOTransformerTest {
	/** The size of the buffer used in testing */
	private final static int BUFFER_SIZE = 4096;
	/** THe function to extract the byte buffer from the object */
	private final static Function<Object, byte[]> messageExtractor = o -> (byte[]) o;

	@Override
	protected byte[] generateOneBuffer() {
		return ByteArrayDataFixture.generateByteData(4096);
	}
	@Override
	protected Transformer setupTransformer() {
		return setupTransformer(BUFFER_SIZE);
	}

	/**
	 * Sets up the transformer with the specified buffer size.
	 * 
	 * @param bufferSize
	 *            the buffer size for the transformer.
	 * @return the configured transformer.
	 */
	private Transformer setupTransformer(int bufferSize) {
		Map<String, String> props = new HashMap<>();
		SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
		setter.transformerBuffer(bufferSize);
		SourceCommonConfig sourceCommonConfig = new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(),
				props);
		return new ByteArrayTransformer(sourceCommonConfig);
	}

	@Override
	@Test
	void testReadData() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				ByteArrayDataFixture.generateByteData(BUFFER_SIZE));
		final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(new ExampleSourceNativeInfo(nativeItem));
		final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

		assertThat(records).extracting(SchemaAndValue::value).extracting(messageExtractor)
				.containsExactly(ByteArrayDataFixture.generateByteData(BUFFER_SIZE));
	}

	@Override
	@Test
	void testReadRecordsSkipFew() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				ByteArrayDataFixture.generateByteRecords(BUFFER_SIZE, 25));
		final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(new ExampleSourceNativeInfo(nativeItem));
		// skip 10 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = (ExampleOffsetManagerEntry) sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(10);
		sourceRecord.setOffsetManagerEntry(entry);

		final List<SchemaAndValue> records = transformer.generateRecords(sourceRecord).toList();
		assertThat(records).hasSize(15);
		for (int i = 0; i < 15; i++) {
			assertThat(records.get(i).value()).isEqualTo(
					ByteArrayDataFixture.generateByteRecord(BUFFER_SIZE, ByteArrayDataFixture.intAsByte(i + 10)));
		}
	}

	@Override
	@Test
	void testReadRecordsSkipMoreRecordsThanExist() throws Exception {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				ByteArrayDataFixture.generateByteRecords(BUFFER_SIZE, 20));
		final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(new ExampleSourceNativeInfo(nativeItem));
		// skip 25 records -- we have to set the record after the read because the
		// getOffsetManagerEntry() creates a defensive copy
		final ExampleOffsetManagerEntry entry = (ExampleOffsetManagerEntry) sourceRecord.getOffsetManagerEntry();
		entry.setRecordCount(25);
		sourceRecord.setOffsetManagerEntry(entry);

		final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

		assertThat(records).isEmpty();
	}

	/**
	 * Test that buffers that are too long wrap correctly. The buffer that is
	 * provided is 10 x {@link #BUFFER_SIZE}. The {@code bufferFactorCount}
	 * determines how many records that will be split into.
	 * 
	 * @param bufferFactorCount
	 *            the factor to multiply the {@link #BUFFER_SIZE} by to create the
	 *            actual test buffer size.
	 * @param numberOfExpectedRecords
	 *            the number of records the byte array is split into based off the
	 *            max buffer size
	 */
	@ParameterizedTest
	@CsvSource({"1,10", "2,5", "3,4", "4,3", "5,2", "6,2", "7,2", "8,2", "9,2", "10,1", "11,1", "12,1"})
	void testGetRecordsWithVariableMaxBufferSize(final int bufferFactorCount, final int numberOfExpectedRecords)
			throws Exception {
		transformer.close();
		int bufferSize = bufferFactorCount * BUFFER_SIZE;
		int byteCount = BUFFER_SIZE * 10;
		transformer = setupTransformer(bufferSize);

		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey",
				ByteArrayDataFixture.generateByteData(byteCount));
		final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(new ExampleSourceNativeInfo(nativeItem));

		final List<SchemaAndValue> records = transformer.generateRecords(sourceRecord).toList();

		assertThat(records).hasSize(numberOfExpectedRecords);
	}
}
