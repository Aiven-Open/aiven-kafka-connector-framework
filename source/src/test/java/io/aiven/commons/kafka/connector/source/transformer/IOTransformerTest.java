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

package io.aiven.commons.kafka.connector.source.transformer;

import io.aiven.commons.kafka.connector.source.impl.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeSourceData;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceRecord;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test for transformers.
 */
public abstract class IOTransformerTest {

	/**
	 * The transformer under test.
	 */
	protected Transformer transformer;

	/**
	 * Setup the transformer for testing.
	 *
	 * @return a configured Transformer.
	 */
	protected abstract Transformer setupTransformer();

	protected abstract byte[] generateOneBuffer() throws IOException;

	@BeforeEach
	final void setUp() {
		transformer = setupTransformer();
	}

	@AfterEach
	final void teardown() throws Exception {
		transformer.close();
	}

	@Test
	abstract void testReadData() throws Exception;

	@Test
	abstract void testReadRecordsSkipFew() throws Exception;

	@Test
	abstract void testReadRecordsSkipMoreRecordsThanExist() throws Exception;

	@Test
	void testIOExceptionDuringCreation() {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
			@Override
			public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
				return () -> {
					throw new IOException("Exception creating stream");
				};
			}
		};
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).isEmpty();
	}

	@Test
	void testIOExceptionDuringDataRead() throws IOException {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
			@Override
			public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
				return () -> new InputStream() {
					@Override
					public int read() throws IOException {
						throw new IOException("Exception reading data stream");
					}
				};
			}
		};
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);
		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);
		assertThat(records).isEmpty();
	}

	@Test
	void testGetRecordsEmptyInputStream() {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData();
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records).isEmpty();
	}

	void verifyCloseCalledAtEnd() throws IOException {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateOneBuffer());
		final CloseTrackingStream[] ctsRef = new CloseTrackingStream[1];
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
			@Override
			public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
				return () -> {
					ctsRef[0] = new CloseTrackingStream(super.getInputStream(sourceRecord).get());
					return ctsRef[0];
				};
			}
		};
		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Stream<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord);

		assertThat(records.count()).isGreaterThan(0);
		assertThat(ctsRef[0].closeCount).isGreaterThan(0);
	}

	void verifyCloseCalledAtIteratorEnd() throws IOException {
		final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateOneBuffer());
		final CloseTrackingStream[] ctsRef = new CloseTrackingStream[1];
		final ExampleNativeSourceData nativeSourceData = new ExampleNativeSourceData() {
			@Override
			public IOSupplier<InputStream> getInputStream(ExampleSourceRecord sourceRecord) {
				return () -> {
					ctsRef[0] = new CloseTrackingStream(super.getInputStream(sourceRecord).get());
					return ctsRef[0];
				};
			}
		};

		final ExampleSourceRecord sourceRecord = new ExampleSourceRecord(nativeItem);

		final Iterator<SchemaAndValue> records = transformer.generateRecords(nativeSourceData, sourceRecord).iterator();
		while (records.hasNext()) {
			records.next();
		}
		assertThat(ctsRef[0].closeCount).isGreaterThan(0);
	}

	private static class CloseTrackingStream extends InputStream {
		InputStream delegate;
		int closeCount;

		CloseTrackingStream(final InputStream stream) {
			super();
			this.delegate = stream;
		}

		@Override
		public int read() throws IOException {
			if (closeCount > 0) {
				throw new IOException("ERROR Read after close");
			}
			return delegate.read();
		}

		@Override
		public void close() throws IOException {
			closeCount++;
			delegate.close();
		}
	}
}
