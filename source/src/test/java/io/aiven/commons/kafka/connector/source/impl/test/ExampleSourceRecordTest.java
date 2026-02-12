package io.aiven.commons.kafka.connector.source.impl.test;

import io.aiven.commons.kafka.connector.source.AbstractSourceRecordTest;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceRecord;

import java.nio.charset.StandardCharsets;

public class ExampleSourceRecordTest
		extends
			AbstractSourceRecordTest<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> {
	@Override
	protected String createKFrom(String key) {
		// the native key is a string.
		return key;
	}

	@Override
	protected ExampleOffsetManagerEntry createOffsetManagerEntry(String key) {
		return new ExampleOffsetManagerEntry(key, "The grouping");
	}

	@Override
	protected ExampleSourceRecord createSourceRecord() {
		return new ExampleSourceRecord(new ExampleNativeItem("the key", "The data".getBytes(StandardCharsets.UTF_8)));
	}
}
