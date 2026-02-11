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
package io.aiven.commons.kafka.connector.source.impl.csv;

import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeClient;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * An actual NativeSourceData implementation would use a NativeClient to
 * retrieve the NativeItems.
 *
 */
public class ExampleCsvSourceData implements NativeSourceData<String> {

	ExampleNativeClient client;

	public ExampleCsvSourceData() throws IOException {
		client = new ExampleNativeClient();
	}

	@Override
	public String getSourceName() {
		return "Example native source data";
	}

	@Override
	public OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Map<String, Object> data) {
		return new ExampleOffsetManagerEntry(data);
	}

	@Override
	protected OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Context context) {
		// cast String because it is the K type in the NativeSourceData<K> above
		return new ExampleOffsetManagerEntry((String) context.getNativeKey(), "Group1");
	}

	@Override
	protected OffsetManager.OffsetManagerKey getOffsetManagerKey(String nativeKey) {
		return new ExampleOffsetManagerEntry(nativeKey, "Group1").getManagerKey();
	}

	@Override
	public Stream<ExampleSourceNativeInfo> getNativeItemStream(String offset) {
		// the offset is a String because it is the K type in NativeSourceData<K> above
		return client.listObjects(offset).stream().map(ExampleSourceNativeInfo::new);
	}

	@Override
	public String parseNativeKey(String keyString) {
		// it is necessary to be able to present the Key as a string. This is the string
		// representation of the K type in NativeSourceData<K> above.
		// the return type is a String because that is the K type in
		// NativeSourceData<K> above
		return keyString;
	}
}
