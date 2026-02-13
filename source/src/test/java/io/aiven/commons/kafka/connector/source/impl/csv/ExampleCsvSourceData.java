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
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A simple String implementation would use a NativeClient to retrieve the
 * NativeItems.
 *
 */
public class ExampleCsvSourceData extends NativeSourceData<String> {

	ExampleCsvClient client;

	public ExampleCsvSourceData(final SourceCommonConfig sourceConfig, final OffsetManager offsetManager)
			throws IOException {
		super(sourceConfig, offsetManager);
		client = new ExampleCsvClient();
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

	/**
	 * extracts the native Key from the string representation.
	 *
	 * @param keyString
	 *            the keyString.
	 * @return The native Key.
	 */
	@Override
	protected String parseNativeKey(String keyString) {
		return "";
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

}
