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
package io.aiven.commons.kafka.connector.source.impl.test;

import io.aiven.commons.kafka.connector.source.integration.AbstractOffsetManagerIntegrationTest;
import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ExampleOffsetManagerIT extends AbstractOffsetManagerIntegrationTest<String, ExampleNativeItem> {
	private static final Logger logger = LoggerFactory.getLogger(ExampleOffsetManagerIT.class);


	private final SourceStorage<String, ExampleNativeItem> sourceStorage;

	public ExampleOffsetManagerIT() {
		sourceStorage = new ExampleSourceStorage();
		sourceStorage.createStorage();
	}

	@Override
	protected SourceStorage<String, ExampleNativeItem> getSourceStorage() {
		return sourceStorage;
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
