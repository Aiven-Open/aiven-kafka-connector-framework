/*
 * Copyright 2024 Aiven Oy
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

import io.aiven.commons.io.compression.CompressionType;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.testFixture.format.JsonTestDataFixture;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Tests the JSON Transformer
 */
final class JsonTransformerTest extends IORecordTransformerTest {

	@Override
	protected Transformer setupTransformer(CompressionType compressionType) {
		Map<String, String> props = new HashMap<>();
		SourceConfigFragment.setter(props).transformerCache(100);
		ConnectorCommonConfigFragment.setter(props).compressionType(compressionType);

		SourceCommonConfig sourceCommonConfig = new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(),
				props);
		return new JsonTransformer(sourceCommonConfig);
	}

	@Override
	protected byte[] generateOneBuffer() throws IOException {
		return JsonTestDataFixture.generateJsonData(1);
	}

	@Override
	protected byte[] generateData(int numberOfRecords) throws IOException {
		return JsonTestDataFixture.generateJsonData(numberOfRecords);
	}

	@Override
	protected String generatedMessagePrefix() {
		return JsonTestDataFixture.MESSAGE_PREFIX;
	}

	@Override
	protected Function<Object, String> messageExtractor() {
		return sv -> ((Map) sv).get("value").toString();
	}
}
