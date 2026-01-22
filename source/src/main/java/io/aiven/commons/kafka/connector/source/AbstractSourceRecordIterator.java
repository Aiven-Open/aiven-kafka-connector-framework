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

package io.aiven.commons.kafka.connector.source;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.kafka.connector.source.task.DistributionStrategy;
import io.aiven.commons.kafka.connector.source.task.DistributionType;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.commons.collections.RingBuffer;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes Native Source Data and creates AbstractSourceRecords.
 * It supports splitting each native record into multiple AbstractSourceRecords.
 * NOTE: this is NOT an {@code abstract} class but rather a {@code final} class
 * that is an iterator over {@link AbstractSourceRecord} instances.
 *
 * @param <N>
 *            the native object type.
 * @param <K>
 *            the key type for the native object.
 * @param <O>
 *            the OffsetManagerEntry for the iterator.
 * @param <T>
 *            The source record for the client type.
 */
public final class AbstractSourceRecordIterator<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>>
		implements
			Iterator<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSourceRecordIterator.class);

	/** The OffsetManager that we are using */
	private final OffsetManager<O> offsetManager;

	/** The configuration for the source */
	private final SourceCommonConfig sourceConfig;

	/** The transformer for the data conversions */
	private final Transformer transformer;

	/** the taskId of this running task */
	private final int taskId;

	/**
	 * The inner iterator to provides a base AbstractSourceRecord for a storage item
	 * that has passed the filters and potentially had data extracted.
	 */
	private Iterator<T> inner;
	/**
	 * The outer iterator that provides an AbstractSourceRecord for each record
	 * contained by the storage item identified by the inner record.
	 */
	private Iterator<T> outer;

	/**
	 * The predicate which will determine if a native item should be assigned to
	 * this task for processing
	 */
	private final Predicate<Optional<T>> taskAssignment;

	private final NativeConverter nativeConverter;

	private final NativeSourceData<K, N, O, T> nativeSourceData;

	/**
	 * The native item key which is currently being processed, when rehydrating from
	 * the storage engine we will skip other native items that come before this key.
	 */
	private K lastSeenNativeKey;
	/**
	 * The ring buffer which contains recently processed native item keys, this is
	 * used during a restart to skip keys that are known to have been processed
	 * while still accounting for the possibility that slower writing to storage may
	 * have introduced newer keys.
	 */
	private final RingBuffer<K> ringBuffer;

	private final K nativeStartKey;

	/**
	 * Constructor.
	 *
	 * @param sourceConfig
	 *            The source configuration.
	 * @param transformer
	 *            The transformer to use for extracting key data and records from
	 *            the native data stream.
	 * @param offsetManager
	 *            the Offset manager to use.
	 * @param nativeSourceData
	 *            Access methods for the native source.
	 */
	public AbstractSourceRecordIterator(final SourceCommonConfig sourceConfig, final Transformer transformer,
			final OffsetManager<O> offsetManager, final NativeSourceData<K, N, O, T> nativeSourceData) {
		super();

		final DistributionType distributionType = sourceConfig.getDistributionType();
		final int maxTasks = sourceConfig.getMaxTasks();
		this.nativeSourceData = nativeSourceData;
		this.sourceConfig = sourceConfig;
		this.transformer = transformer;
		this.offsetManager = offsetManager;
		this.taskId = sourceConfig.getTaskId() % maxTasks;
		this.taskAssignment = new TaskAssignment(distributionType.getDistributionStrategy(maxTasks));
		this.nativeConverter = new NativeConverter();
		this.nativeStartKey = sourceConfig.getNativeStartKey() != null
				? nativeSourceData.parseNativeKey(sourceConfig.getNativeStartKey())
				: null;
		this.inner = Collections.emptyIterator();
		this.outer = Collections.emptyIterator();
		this.ringBuffer = new RingBuffer<>(sourceConfig.getRingBufferSize());
	}

	/**
	 * Gets the logger for the concrete implementation.
	 *
	 * @return The logger for the concrete implementation.
	 */

	@Override
	public boolean hasNext() {
		if (!outer.hasNext() && lastSeenNativeKey != null) {
			// update the buffer to contain this new objectKey
			ringBuffer.add(lastSeenNativeKey);
			// Remove the last seen from the offset manager as the file has been completely
			// processed.
			offsetManager.removeEntry(nativeSourceData.getOffsetManagerKey(lastSeenNativeKey));
		}
		if (!inner.hasNext() && !outer.hasNext()) {
			inner = nativeSourceData.getNativeItemStream(ObjectUtils.getIfNull(ringBuffer.getNextEjected(), () -> {
				LOGGER.info("{} set, no alternative present in buffer will begin consuming from {}",
						SourceConfigFragment.NATIVE_START_KEY, nativeStartKey);
				return nativeStartKey;
			})).map(nativeConverter).filter(taskAssignment).filter(Optional::isPresent).map(optT -> {
				T sourceRecord = optT.get();
				lastSeenNativeKey = sourceRecord.getNativeKey();
				return sourceRecord;
			}).iterator();
		}
		while (!outer.hasNext() && inner.hasNext()) {
			outer = convert(inner.next()).iterator();
		}
		return outer.hasNext();
	}

	@Override
	public T next() {
		return outer.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("This iterator is unmodifiable");
	}

	/**
	 * Converts the native item into stream of AbstractSourceRecords.
	 *
	 * @param sourceRecord
	 *            the SourceRecord that drives the creation of source records with
	 *            values.
	 * @return a stream of T created from the input stream of the native item.
	 */
	private Stream<T> convert(final T sourceRecord) {
		sourceRecord
				.setKeyData(transformer.getKeyData(sourceRecord.getNativeKey(), sourceRecord.getTopic(), sourceConfig));
		lastSeenNativeKey = sourceRecord.getNativeKey();
		return transformer.getRecords(nativeSourceData, sourceRecord, sourceConfig).map(new Mapper<>(sourceRecord));
	}

	/**
	 * Maps the data from the @{link Transformer} stream to an AbstractSourceRecord
	 * given all the additional data required.
	 *
	 * @param <N>
	 *            the native object type.
	 * @param <K>
	 *            the key type for the native object.
	 * @param <O>
	 *            the OffsetManagerEntry for the iterator.
	 * @param <T>
	 *            The source record for the client type.
	 */
	private static class Mapper<N, K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>>
			implements
				Function<SchemaAndValue, T> {
		/**
		 * The AbstractSourceRecord that produces the values.
		 */
		private final T sourceRecord;

		/**
		 * Constructor.
		 *
		 * @param sourceRecord
		 *            The source record to provide default values..
		 */
		public Mapper(final T sourceRecord) {
			// operation within the Transformer
			// to see if there are more records.
			this.sourceRecord = sourceRecord;
		}

		@Override
		public T apply(final SchemaAndValue valueData) {
			sourceRecord.incrementRecordCount();
			final T result = sourceRecord.duplicate();
			result.setValueData(valueData);
			return result;
		}
	}

	/**
	 * Determines if an AbstractSourceRecord belongs to this task.
	 */
	private class TaskAssignment implements Predicate<Optional<T>> {
		private final DistributionStrategy distributionStrategy;
		/**
		 * Constructs a task assignment from the distribution strategy.
		 *
		 * @param distributionStrategy
		 *            The distribution strategy.
		 */
		TaskAssignment(final DistributionStrategy distributionStrategy) {
			this.distributionStrategy = distributionStrategy;
		}

		@Override
		public boolean test(final Optional<T> optionalSourceRecord) {
			return optionalSourceRecord
					.map(sourceRecord -> taskId == distributionStrategy.getTaskFor(sourceRecord.getContext()))
					.orElse(false);
		}
	}

	/**
	 * Converts the native item to an AbstractSourceRecord while performing other
	 * housekeeping functions.
	 */
	private class NativeConverter implements Function<N, Optional<T>> {

		@Override
		public Optional<T> apply(final N nativeItem) {
			final K itemName = nativeSourceData.getNativeKey(nativeItem);
			final Optional<Context<K>> optionalContext = nativeSourceData.extractContext(nativeItem);
			if (optionalContext.isPresent() && !ringBuffer.contains(itemName)) {
				final T sourceRecord = nativeSourceData.createSourceRecord(nativeItem);
				final Context<K> context = optionalContext.get();
				overrideContextTopic(context);
				sourceRecord.setContext(context);
				O offsetManagerEntry = nativeSourceData.createOffsetManagerEntry(nativeItem);
				offsetManagerEntry = offsetManager
						.getEntry(offsetManagerEntry.getManagerKey(), offsetManagerEntry::fromProperties)
						.orElse(offsetManagerEntry);
				sourceRecord.setOffsetManagerEntry(offsetManagerEntry);
				return Optional.of(sourceRecord);
			}
			return Optional.empty();
		}

		/**
		 * Sets the target topic in the context.
		 *
		 * @param context
		 *            the context to set the topic in if found.
		 */
		private void overrideContextTopic(final Context<K> context) {
			final String targetTopic = sourceConfig.getTargetTopic();
			if (targetTopic != null) {
				if (context.getTopic().isPresent()) {
					LOGGER.debug(
							"Overriding topic '{}' extracted from native item name with topic '{}' from configuration 'topics'. ",
							context.getTopic().get(), targetTopic);
				}
				context.setTopic(targetTopic);
			}
		}
	}
}
