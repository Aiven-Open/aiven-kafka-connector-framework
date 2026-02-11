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
package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.connector.source.AbstractSourceRecordIterator;
import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.transformer.JsonTransformer;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.Map;

public class ExampleSourceTask
		extends
			AbstractSourceTask<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> {
	private ExampleNativeSourceData nativeSourceData;

	public ExampleSourceTask() {
	}

	@Override
	protected AbstractSourceRecordIterator<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> getIterator(
			SourceCommonConfig config) {
		return new AbstractSourceRecordIterator<>(config, new OffsetManager<>(context), nativeSourceData);
	}

	@Override
	protected SourceCommonConfig configure(Map<String, String> props) {
		try {
			nativeSourceData = new ExampleNativeSourceData();
		} catch (IOException e) {
			throw new ConfigException("Unable to create native source data", e);
		}
		SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
		setter.transformerClass(JsonTransformer.class);
		return new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
	}

	@Override
	protected void closeResources() {

	}

	@Override
	public String version() {
		return "Example version 1.0";
	}
}
