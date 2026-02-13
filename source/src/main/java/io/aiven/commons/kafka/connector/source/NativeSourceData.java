/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.commons.kafka.connector.source;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.commons.collections.RingBuffer;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A data source for native info from a source.
 * 
 * @param <K>
 *            the key type for the native object.
 */
public abstract class NativeSourceData<K extends Comparable<K>> implements AutoCloseable {

	private static final Logger LOGGER = LoggerFactory.getLogger(NativeSourceData.class);

	/**
	 * The start key.
	 */
	private final K startKey;

	/**
	 * The last seen native key.
	 */
	private K lastSeenNativeKey;

	/**
	 * The offset manager.
	 */
	private final OffsetManager offsetManager;

	/**
	 * The source common config
	 */
	private final SourceCommonConfig sourceConfig;

	/**
	 * The transformer to use.
	 */
	private final Transformer transformer;

	/**
	 * The ring buffer which contains recently processed native item keys, this is
	 * used during a restart to skip keys that are known to have been processed
	 * while still accounting for the possibility that slower writing to storage may
	 * have introduced newer keys.
	 */
	private final RingBuffer<K> ringBuffer;

	/**
	 * Constructor
	 *
	 * @param sourceConfig
	 *            the source configuration for the native source.
	 * @param offsetManager
	 *            The offset manager from the kafka task.
	 */
	protected NativeSourceData(final SourceCommonConfig sourceConfig, final OffsetManager offsetManager) {
		this.sourceConfig = sourceConfig;
		this.ringBuffer = new RingBuffer<>(sourceConfig.getRingBufferSize());
		this.offsetManager = offsetManager;
		this.transformer = sourceConfig.getTransformer();
		this.startKey = sourceConfig.getNativeStartKey() != null
				? parseNativeKey(sourceConfig.getNativeStartKey())
				: null;
	}

	/**
	 * Gets the name for the source type.
	 *
	 * @return the common name for the data source. For example "AWS S3 Storage" or
	 *         "AMQP Stream source".
	 */
	public abstract String getSourceName();

	/**
	 * Get a stream of Native object from the underlying storage layer. The
	 * implementation must return the native objects in a repeatable order based on
	 * the key. In addition, the underlying storage must be able to start streaming
	 * from a specific previously returned key.
	 *
	 * @param offset
	 *            the native key to start from. May be {@code null} ot indicate
	 *            start at the beginning.
	 * @return A stream of native objects. May be empty but not {@code null}.
	 */
	protected abstract Stream<? extends AbstractSourceNativeInfo<K, ?>> getNativeItemStream(final K offset);

	/**
	 * Creates an offset manager entry using the data in the map.
	 *
	 * @param data
	 *            the data to create the offset manager from.
	 * @return a valid offset manager entry.
	 */
	public abstract OffsetManager.OffsetManagerEntry createOffsetManagerEntry(final Map<String, Object> data);

	/**
	 * Creates an offset manager entry from a context.
	 *
	 * @param context
	 *            the context to create the offset manager from.
	 * @return a valid offset manager.
	 */
	protected abstract OffsetManager.OffsetManagerEntry createOffsetManagerEntry(final Context context);

	/**
	 * Converts a source native info into an evolving record while filtering out
	 * records that have already been processed (are in the ringBuffer).
	 *
	 * @param sourceNativeInfo
	 *            the source native info to convert
	 * @return a valid Evolving Source Record or an empty optional.
	 */
	private Optional<EvolvingSourceRecord> nativeConverter(final AbstractSourceNativeInfo<K, ?> sourceNativeInfo) {
		if (!ringBuffer.contains(sourceNativeInfo.nativeKey())) {
			final Context context = overrideContextTopic(sourceNativeInfo.getContext());
			OffsetManager.OffsetManagerEntry offsetManagerEntry = createOffsetManagerEntry(context);
			offsetManagerEntry = offsetManager.getEntryData(offsetManagerEntry.getManagerKey())
					.map(this::createOffsetManagerEntry).orElse(offsetManagerEntry);
			return Optional.of(new EvolvingSourceRecord(sourceNativeInfo, offsetManagerEntry, context));
		}
		return Optional.empty();
	}

	/**
	 * Sets the target topic in the context if the target topic is set in the
	 * configuration.
	 *
	 * @param context
	 *            the context to set the topic in if found.
	 */
	private Context overrideContextTopic(final Context context) {
		final String targetTopic = sourceConfig.getTargetTopic();
		if (targetTopic != null) {
			if (context.getTopic().isPresent()) {
				LOGGER.debug(
						"Overriding topic '{}' extracted from native item name with topic '{}' from configuration 'topics'. ",
						context.getTopic().get(), targetTopic);
			}
			context.setTopic(targetTopic);
		}
		return context;
	}

	/**
	 * Constructs a new iterator to continue extracting data from the native
	 * storage. Iterator is constructed by calling {@link #getNativeItemStream} and
	 * passing the native key from which we want to start scanning. This will be
	 * <ol>
	 * <li>The oldest record in the ringBuffer; or</li>
	 * <li>the startKey defined in the configuration file; or</li>
	 * <li>{@code null}</li>
	 * </ol>
	 *
	 * @param isCorrectTask
	 *            A predicate to test if the context for the enclosed records is for
	 *            this task.
	 * @return an Iterator on EvolvingSourceRecords.
	 */
	final Iterator<EvolvingSourceRecord> getIterator(final Predicate<Context> isCorrectTask) {
		return getNativeItemStream(ObjectUtils.getIfNull(ringBuffer.getNextEjected(), () -> {

			LOGGER.info("{} set, no alternative present in buffer will begin consuming from {}",
					SourceConfigFragment.NATIVE_START_KEY, startKey);
			return startKey;
		})).map(this::nativeConverter).filter(osr -> osr.map(sr -> isCorrectTask.test(sr.getContext())).orElse(false))
				.map(optT -> {
					EvolvingSourceRecord sourceRecord = optT.get();
					lastSeenNativeKey = (K) sourceRecord.getNativeKey();
					return sourceRecord;
				}).iterator();
	}

	/**
	 * Converts the native item into stream of AbstractSourceRecords.
	 *
	 * @param sourceRecord
	 *            the SourceRecord that drives the creation of source records with
	 *            values.
	 * @return a stream of T created from the input stream of the native item.
	 */
	final Stream<EvolvingSourceRecord> transform(final EvolvingSourceRecord sourceRecord) {
		sourceRecord.setKeyData(transformer.generateKeyData(sourceRecord));
		// lastSeenNativeKey = ((AbstractSourceNativeInfo<K, N>)
		// sourceRecord.nativeInfo).nativeInfo.nativeKey();
		return transformer.generateRecords(sourceRecord).map(new Mapper(sourceRecord));
	}

	/**
	 * extracts the native Key from the string representation.
	 *
	 * @param keyString
	 *            the keyString.
	 * @return The native Key.
	 */
	protected abstract K parseNativeKey(final String keyString);

	/**
	 * Creates an offset manager key for the native key.
	 *
	 * @param nativeKey
	 *            THe native key to create an offset manager key for.
	 * @return An offset manager key.
	 */
	protected abstract OffsetManager.OffsetManagerKey getOffsetManagerKey(final K nativeKey);

	@Override
	public void close() throws Exception {
		transformer.close();
	}

	/**
	 * Updates the ring buffer with the last native key processed and removes the
	 * lastSeenNative key from the local copy of the offsetManager data.
	 */
	void recordNativeKeyFinished() {
		if (lastSeenNativeKey != null) {
			// update the buffer to contain this new objectKey
			ringBuffer.add(lastSeenNativeKey);
			// Remove the last seen from the offset manager as the file has been completely
			// processed.
			offsetManager.removeEntry(getOffsetManagerKey(lastSeenNativeKey));
		}
	}

	/**
	 * Maps the data from the @{link Transformer} stream to an EvolvingSourceRecord
	 * given all the additional data required.
	 * <p>
	 * The record count in the source record is updated.
	 *
	 * @param sourceRecord
	 *            The EvolvingSourceRecord that produces the values.
	 */
	private record Mapper(EvolvingSourceRecord sourceRecord) implements Function<SchemaAndValue, EvolvingSourceRecord> {
		@Override
		public EvolvingSourceRecord apply(final SchemaAndValue valueData) {
			sourceRecord.incrementRecordCount();
			final EvolvingSourceRecord result = new EvolvingSourceRecord(sourceRecord);
			result.setValueData(valueData);
			return result;
		}
	}
}
