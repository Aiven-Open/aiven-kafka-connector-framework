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

package io.aiven.commons.kafka.connector.source.transformer;

import io.aiven.commons.kafka.source.AbstractSourceRecord;
import io.aiven.commons.kafka.source.NativeSourceData;
import io.aiven.commons.kafka.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.source.task.Context;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Converts input stream from native source into a byte stream that can be
 * processed by the connector.
 */
public abstract class Transformer {
	/**
	 * Returned if the transformer can not determine the length of the stream.
	 */
	public final static long UNKNOWN_STREAM_LENGTH = -1;

	/**
	 * Constructor.
	 */
	protected Transformer() {
	}
	/**
	 * Gets a stream of SchemaAndValue records from the input stream.
	 * 
	 * @param nativeSourceData
	 *            The native source data.
	 * @param sourceRecord
	 *            The AbstractSourceRecord being processed.
	 * @param sourceConfig
	 *            The source configuration for the Kafka task.
	 * @param <T>
	 *            The concrete class of the AbstractSourceRecord.
	 * @return the stream of values for Kafka SourceRecords.
	 */
	public <T extends AbstractSourceRecord<?, ?, ?, T>> Stream<SchemaAndValue> getRecords(
			final NativeSourceData<?, ?, ?, T> nativeSourceData, final T sourceRecord,
			final SourceCommonConfig sourceConfig) {

		final StreamSpliterator spliterator = createSpliterator(nativeSourceData.getInputStream(sourceRecord),
				sourceRecord.getNativeItemSize(), sourceRecord.getContext(), sourceConfig);
		return StreamSupport.stream(spliterator, false).onClose(spliterator::close).skip(sourceRecord.getRecordCount());
	}

	/**
	 * Creates the stream spliterator for this transformer.
	 *
	 * @param inputStreamIOSupplier
	 *            the input stream supplier.
	 * @param streamLength
	 *            the length of the input stream, {@link #UNKNOWN_STREAM_LENGTH} may
	 *            be used to specify a stream with an unknown length, streams of
	 *            length zero will log an error and return an empty stream
	 * @param context
	 *            the context
	 * @param sourceConfig
	 *            the source configuration.
	 * @return a StreamSpliterator instance.
	 */
	protected abstract StreamSpliterator createSpliterator(IOSupplier<InputStream> inputStreamIOSupplier,
			long streamLength, Context<?> context, SourceCommonConfig sourceConfig);

	/**
	 * Convert the native key into a Schema and Value for Kafka.
	 * 
	 * @param nativeKey
	 *            the native key to convert.
	 * @param topic
	 *            the topic for the conversion.
	 * @param sourceConfig
	 *            the SourceCommonConfig for the conversion.
	 * @return a SchemaAndValue for the key.
	 */
	public abstract SchemaAndValue getKeyData(Object nativeKey, String topic, SourceCommonConfig sourceConfig);

	/**
	 * A Spliterator that performs various checks on the opening/closing of the
	 * input stream.
	 */
	public abstract static class StreamSpliterator implements Spliterator<SchemaAndValue> {
		/**
		 * The input stream supplier.
		 */
		private final IOSupplier<InputStream> inputStreamIOSupplier;
		/**
		 * The logger to be used by all instances of this class. This will be the
		 * Transformer logger.
		 */
		protected final Logger logger;
		/**
		 * * The input stream. Will be null until {@link #inputOpened} has completed.
		 * May be used for reading but should not be closed or otherwise made
		 * unreadable.
		 */
		protected InputStream inputStream;

		/**
		 * A flag indicate that the input stream has been closed.
		 */
		private boolean closed;

		/**
		 * Constructor.
		 *
		 * @param logger
		 *            The logger for this Spliterator to use.
		 * @param inputStreamIOSupplier
		 *            the InputStream supplier
		 */
		protected StreamSpliterator(final Logger logger, final IOSupplier<InputStream> inputStreamIOSupplier) {
			this.logger = logger;
			this.inputStreamIOSupplier = inputStreamIOSupplier;
		}

		/**
		 * Attempt to read the next record. If there is no record to read or an error
		 * occurred return false. If a record was created, call {@code action.accept()}
		 * with the record.
		 *
		 * @param action
		 *            the Consumer to call if record is created.
		 * @return {@code true} if a record was processed, {@code false} otherwise.
		 */
		abstract protected boolean doAdvance(Consumer<? super SchemaAndValue> action);

		/**
		 * Method to close additional inputs if needed.
		 */
		abstract protected void doClose();

		/**
		 * Closes the StreamSplitter by calling {@link #doClose()} and then closing the
		 * input stream.
		 */
		public final void close() {
			doClose();
			try {
				if (inputStream != null) {
					inputStream.close();
					inputStream = null; // NOPMD setting null to release resources
					closed = true;
				}
			} catch (IOException e) {
				logger.error("Error trying to close inputStream: {}", e.getMessage(), e);
			}
		}

		/**
		 * Allows modification of input stream. Called immediately after the input
		 * stream is opened. Implementations may modify the type of input stream by
		 * wrapping it with a specific implementation, or may create Readers from the
		 * input stream. The modified input stream must be returned. If a Reader or
		 * similar class is created from the input stream the input stream must be
		 * returned. The {@link #inputStream} instance variable will be null until
		 * {@code inputOpened} has completed. The implementation of the interface is
		 * responsible for closing any newly constructed readers or input streams in the
		 * {@link #doClose()} method.
		 *
		 * @param input
		 *            the input stream that was just opened.
		 * @throws IOException
		 *             on IO error.
		 */
		abstract protected void inputOpened(InputStream input) throws IOException;

		@Override
		public final boolean tryAdvance(final Consumer<? super SchemaAndValue> action) {
			if (closed) {
				return false;
			}
			boolean result = false;
			try {
				if (inputStream == null) {
					try {
						inputStream = inputStreamIOSupplier.get();
						inputOpened(inputStream);
					} catch (IOException e) {
						logger.error("Error trying to open inputStream: {}", e.getMessage(), e);
						close();
						return false;
					}
				}
				result = doAdvance(action);
			} catch (RuntimeException e) {
				logger.error("Error trying to advance data: {}", e.getMessage(), e);
			}
			if (!result) {
				close();
			}
			return result;
		}

		@Override
		public final Spliterator<SchemaAndValue> trySplit() { // NOPMD returning null is reqruied by API
			return null;
		}

		@Override
		public long estimateSize() {
			return Long.MAX_VALUE;
		}

		@Override
		public int characteristics() {
			return ORDERED | NONNULL;
		}
	}
}
