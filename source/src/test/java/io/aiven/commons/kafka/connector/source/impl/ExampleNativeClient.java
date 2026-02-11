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

import io.aiven.commons.kafka.connector.source.testFixture.format.JsonTestDataFixture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A "native" client. This client returns lists of native objects and
 * ByteBuffers for specific native keys.
 *
 * In an actual implementation this would connect to the storage and retrieve
 * data.
 */
public class ExampleNativeClient {
	public final Map<String, ExampleNativeItem> dataMap;
	public String dataSent;

	public ExampleNativeClient() throws IOException {
		dataMap = new LinkedHashMap<>();
		dataMap.put("first 10", new ExampleNativeItem("first 10", JsonTestDataFixture.generateJsonData(10)));
		dataMap.put("second 10", new ExampleNativeItem("second 10", JsonTestDataFixture.generateJsonData(10, 10)));
		dataMap.put("third 10", new ExampleNativeItem("third 10", JsonTestDataFixture.generateJsonData(20, 10)));
	}

	/**
	 * Gets a list of native objects. In an actual implementation this method may
	 * retrieve one object or may retrieve a number of objects.
	 *
	 * @param offset
	 *            This is the key value that was last read or null if there is no
	 *            last read. It is a String because the K in the
	 *            {@code NativeSourceData<K,N,O,T>} as defined in
	 *            {@link ExampleNativeSourceData} is a String
	 * @return the list of native objects. This is a collection of ExampleNativeItem
	 *         because that is the type of the N in
	 *         {@code NativeSourceData<K,N,O,T>}
	 *
	 */
	Collection<ExampleNativeItem> listObjects(String offset) {
		/*
		 * In real life the calling framework tracks what has been processed via the
		 * OffsetManager so sending the same data multiple times in not an issue.
		 * However, in this simple version we are not tracking the offsets so we only
		 * send the block of data once. This example simulates returning 2 items, and
		 * after they are processed finding the third. After the third item is processed
		 * there are no more data elements.
		 */
		if (dataSent == null) {
			dataSent = "second 10";
			return Arrays.asList(dataMap.get("first 10"), dataMap.get("second 10"));
		} else if (dataSent.equals("second 10")) {
			dataSent = "third 10";
			return Collections.singletonList(dataMap.get("third 10"));
		}
		return Collections.emptyList();
	}

	/**
	 * Gets the ByteBuffer for a key.
	 *
	 * @param key
	 *            the key to get the byte buffer for.
	 * @return The ByteBuffer for a key, or {@code null} if not set.
	 */
	ByteBuffer getObjectAsBytes(String key) {
		ExampleNativeItem nativeItem = dataMap.get(key);
		return nativeItem == null ? null : nativeItem.data();
	};
}
