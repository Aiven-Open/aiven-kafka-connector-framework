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

import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.apache.commons.io.function.IOSupplier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * An actual NativeSourceData implementation would use a NativeClient to
 * retrieve the NativeItems.
 *
 */
public class ExampleNativeSourceData
		implements
			NativeSourceData<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> {

	ExampleNativeClient client;

	public ExampleNativeSourceData() throws IOException {
		client = new ExampleNativeClient();
	}

	@Override
	public String getSourceName() {
		return "Example native source data";
	}

	@Override
	public Stream<ExampleNativeItem> getNativeItemStream(String offset) {
		// the offset is a String because it is the K type in NativeSourceData<K,N,O,T>
		// above
		return client.listObjects(offset).stream();
	}

	@Override
	public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
		// the sourceRecord is an ExampleSourceRecord because that is the T type in
		// NativeSourceData<K,N,O,T> above
		return () -> new ByteArrayInputStream(sourceRecord.getNativeItem().data().array());
	}

	@Override
	public String getNativeKey(ExampleNativeItem nativeItem) {
		// the nativeItem is an ExampleSourceItem because that is the N type in
		// NativeSourceData<K,N,O,T> above
		// the return type is a String because that is the K type in
		// NativeSourceData<K,N,O,T> above
		return nativeItem.key();
	}

	@Override
	public String parseNativeKey(String keyString) {
		// it is necessary to be able to present the Key as a string. This is the string
		// representation of the K type in NativeSourceData<K,N,O,T> above.
		// the return type is a String because that is the K type in
		// NativeSourceData<K,N,O,T> above
		return keyString;
	}

	@Override
	public ExampleSourceRecord createSourceRecord(ExampleNativeItem nativeItem) {
		// the nativeItem is an ExampleSourceItem because that is the N type in
		// NativeSourceData<K,N,O,T> above
		// the return type is an ExampleSourceRecord because that is the T type in
		// NativeSourceData<K,N,O,T> above
		return new ExampleSourceRecord(nativeItem);
	}

	@Override
	public ExampleOffsetManagerEntry createOffsetManagerEntry(ExampleNativeItem nativeItem) {
		// the nativeItem is an ExampleSourceItem because that is the N type in
		// NativeSourceData<K,N,O,T> above
		// the return type is an ExampleOffsetManagerEntry because that is the O type in
		// NativeSourceData<K,N,O,T> above
		// the OffsetManagerEntry is a case where the native key needs to be represented
		// as a string
		return new ExampleOffsetManagerEntry(nativeItem.key(), "group1");
	}

	@Override
	public OffsetManager.OffsetManagerKey getOffsetManagerKey(String nativeKey) {
		// the nativeKey is a string because that is the K type in in
		// NativeSourceData<K,N,O,T> above
		return new ExampleOffsetManagerEntry(nativeKey, "group1").getManagerKey();
	}

	@Override
	public Optional<Context<String>> extractContext(ExampleNativeItem nativeItem) {
		// the nativeKey is a string because that is the K type in in
		// NativeSourceData<K,N,O,T> above
		// This is where Contextual information can be extracted from the nativeItem. At
		// a minimum the
		// nativeKey must be provided. See the Context class for other properties that
		// can be set.
		return Optional.of(new Context<>(getNativeKey(nativeItem)));
	}
}
