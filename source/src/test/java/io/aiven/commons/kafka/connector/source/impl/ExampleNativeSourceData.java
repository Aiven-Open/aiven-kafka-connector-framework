package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.apache.commons.io.function.IOSupplier;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;

public class ExampleNativeSourceData  implements NativeSourceData<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> {

		IOSupplier<InputStream> streamSupplier;
		public ExampleNativeSourceData() {
		}

		@Override
		public String getSourceName() {
			return "Example native source data";
		}

		@Override
		public Stream<ExampleNativeItem> getNativeItemStream(String offset) {
			return Stream.empty();
		}

		@Override
		public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
			return () -> new ByteArrayInputStream(sourceRecord.getNativeItem().data.array());
		}

		@Override
		public String getNativeKey(ExampleNativeItem nativeItem) {
			return nativeItem.key;
		}

		@Override
		public String parseNativeKey(String keyString) {
			return keyString;
		}

		@Override
		public ExampleSourceRecord createSourceRecord(ExampleNativeItem nativeItem) {
			return new ExampleSourceRecord(nativeItem);
		}

		@Override
		public ExampleOffsetManagerEntry createOffsetManagerEntry(ExampleNativeItem nativeItem) {
			return new ExampleOffsetManagerEntry(nativeItem.key, "group1");
		}

		@Override
		public OffsetManager.OffsetManagerKey getOffsetManagerKey(String nativeKey) {
			return new ExampleOffsetManagerEntry(nativeKey, "group1").getManagerKey();
		}

		@Override
		public Optional<Context<String>> extractContext(ExampleNativeItem nativeItem) {
			return Optional.empty();
		}
	}

