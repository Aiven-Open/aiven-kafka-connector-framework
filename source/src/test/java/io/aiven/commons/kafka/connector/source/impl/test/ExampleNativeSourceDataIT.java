package io.aiven.commons.kafka.connector.source.impl.test;

import io.aiven.commons.kafka.connector.source.AbstractNativeSourceDataIntegrationTest;
import io.aiven.commons.kafka.connector.source.AbstractSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class ExampleNativeSourceDataIT extends AbstractNativeSourceDataIntegrationTest<String, ExampleNativeItem> {
	private final ExampleSourceStorage storage;
	private final Map<String, String> properties = new HashMap<>();
	private final SourceCommonConfig sourceConfig = new SourceCommonConfig(
			new SourceCommonConfig.SourceCommonConfigDef(), properties);
	private final OffsetManager offsetManager = mock(OffsetManager.class);

	public ExampleNativeSourceDataIT() {
		storage = new ExampleSourceStorage();
		storage.createStorage();
	}
	@Override
	protected NativeSourceData<String> getNativeSourceData() {
		return new NativeSourceData<String>(sourceConfig, offsetManager) {
			@Override
			public String getSourceName() {
				return "ExampleSource";
			}

			@Override
			protected Stream<? extends AbstractSourceNativeInfo<String, ?>> getNativeItemStream(String offset) {
				return storage.client.listObjects(offset).stream().map(ExampleSourceNativeInfo::new);
			}

			@Override
			public OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Map<String, Object> data) {
				return new ExampleOffsetManagerEntry(data);
			}

			@Override
			protected OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Context context) {
				return new ExampleOffsetManagerEntry((String) context.getNativeKey(), "grouping1");
			}

			@Override
			protected Optional<KeySerde<String>> getNativeKeySerde() {
				return Optional.of(KeySerde.STRING_SERDE);
			}

			@Override
			protected OffsetManager.OffsetManagerKey getOffsetManagerKey(String nativeKey) {
				return new ExampleOffsetManagerEntry(nativeKey, "grouping1").getManagerKey();
			}
		};
	}

	@Override
	protected SourceStorage<String, ExampleNativeItem> getSourceStorage() {
		return storage;
	}

}
