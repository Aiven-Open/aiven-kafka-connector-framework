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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.testFixture.format.CsvTestDataFixture;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

final class CsvTransformerTest extends IORecordTransformerTest {

	@Override
	protected Transformer setupTransformer() {
		Map<String, String> props = new HashMap<>();
		SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
		setter.transformerCache(100);
		SourceCommonConfig sourceCommonConfig = new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(),
				props);
		return new CsvTransformer(sourceCommonConfig);
	}

	/**
	 * Generate one buffer for the transformer to read. This must be a valid data
	 * buffer for the Transformer under test.
	 *
	 * @return the buffer to read.
	 */
	@Override
	protected byte[] generateOneBuffer() {
		return CsvTestDataFixture.generateCsvData(1);
	}

	/**
	 * Get the string prefix for the data messages.
	 *
	 * @return the string prefix for the data messages.
	 */
	@Override
	protected String generatedMessagePrefix() {
		return "Hello, from Csv the Test Data Fixture! Object ";
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
	@Override
	protected byte[] generateData(int numberOfRecords) throws IOException {
		return CsvTestDataFixture.generateCsvData(0, numberOfRecords);
	}

	/**
	 * Given a value object from a SchemaAndValue object extract the message from
	 * it.
	 *
	 * @return the message to extract.
	 */
	@Override
	protected Function<Object, String> messageExtractor() {
		return sv -> {
			try {
				return new ObjectMapper().writeValueAsString(((Struct) sv).getMap("value"));
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		};
	}
}
