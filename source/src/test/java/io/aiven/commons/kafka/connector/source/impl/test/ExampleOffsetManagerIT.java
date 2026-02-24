package io.aiven.commons.kafka.connector.source.impl.test;

import io.aiven.commons.kafka.connector.source.integration.AbstractOffsetManagerIntegrationTest;
import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ExampleOffsetManagerIT extends AbstractOffsetManagerIntegrationTest<String, ByteBuffer> {
	private static final Logger logger = LoggerFactory.getLogger(ExampleOffsetManagerIT.class);
	@Override
	protected SourceStorage<String, ByteBuffer> getSourceStorage() {
		SourceStorage<String, ByteBuffer> result = new ExampleSourceStorage();
		result.createStorage();
		return result;
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
