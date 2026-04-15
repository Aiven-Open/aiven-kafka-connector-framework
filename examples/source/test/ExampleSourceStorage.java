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

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceConnector;
import io.aiven.commons.kafka.connector.source.impl.nativeProvided.ExampleNativeClient;
import io.aiven.commons.kafka.connector.source.impl.nativeProvided.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorRegistry;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.connector.Connector;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class ExampleSourceStorage implements SourceStorage<String, ExampleNativeItem> {
	ExampleNativeClient client;

	@Override
	public ExtractorRegistry supportedTransformers() {
		return null;
	}

	@Override
	public String createKey(String topic, int partition) {
		return String.format("%s-%s", System.currentTimeMillis(), UUID.randomUUID());
	}

	@Override
	public WriteResult<String> writeWithKey(String nativeKey, byte[] testDataBytes) {
		client.write(nativeKey, testDataBytes);
		OffsetManager.OffsetManagerKey offsetManagerKey = new ExampleOffsetManagerEntry(nativeKey, "grouping")
				.getManagerKey();
		return new WriteResult<>(offsetManagerKey, nativeKey);
	}

	@Override
	public Map<String, String> createConnectorConfig() {
		return new HashMap<>();
	}

	@Override
	public BiFunction<Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry> offsetManagerEntryFactory() {
		return (k, v) -> new ExampleOffsetManagerEntry(v);
	}

	@Override
	public Class<? extends Connector> getConnectorClass() {
		return ExampleSourceConnector.class;
	}

	@Override
	public void createStorage() {
		client = new ExampleNativeClient();
	}

	@Override
	public void removeStorage() {
		client.clear();
	}

	@Override
	public List<? extends NativeInfo<String, ExampleNativeItem>> getNativeInfo() {
		return client.listObjects().stream().map(e -> new NativeInfo<String, ExampleNativeItem>(e.key(), e))
				.collect(Collectors.toList());
	}

	@Override
	public IOSupplier<InputStream> getInputStream(String nativeKey) {
		return null;
	}

	@Override
	public String defaultPrefix() {
		return "";
	}
}
