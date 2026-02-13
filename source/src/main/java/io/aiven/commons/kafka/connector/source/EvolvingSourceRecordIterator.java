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

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.DistributionStrategy;

/**
 * Iterator that processes Native Source Data and creates AbstractSourceRecords.
 * It supports splitting each native record into multiple AbstractSourceRecords.
 * NOTE: this is NOT an {@code abstract} class but rather a {@code final} class
 * that is an iterator over {@link EvolvingSourceRecord} instances.
 */
public final class EvolvingSourceRecordIterator implements Iterator<EvolvingSourceRecord> {

	/** the taskId of this running task */
	private final int taskId;

	/**
	 * The inner iterator to provides a base EvolvingSourceRecord for a storage item
	 * that has passed the filters and potentially had data extracted.
	 */
	private Iterator<EvolvingSourceRecord> inner;
	/**
	 * The outer iterator that provides an EvolvingSourceRecord for each record
	 * contained by the storage item identified by the inner record.
	 */
	private Iterator<EvolvingSourceRecord> outer;

	/**
	 * The predicate which will determine if a native item should be assigned to
	 * this task for processing
	 */
	private final DistributionStrategy distributionStrategy;

	private final NativeSourceData<?> nativeSourceData;

	/**
	 * Constructor.
	 *
	 * @param sourceConfig
	 *            The source configuration.
	 * @param nativeSourceData
	 *            Access methods for the native source.
	 */
	public EvolvingSourceRecordIterator(final SourceCommonConfig sourceConfig,
			final NativeSourceData<?> nativeSourceData) {
		super();
		final int maxTasks = sourceConfig.getMaxTasks();
		this.nativeSourceData = nativeSourceData;
		this.taskId = sourceConfig.getTaskId() % maxTasks;
		this.distributionStrategy = sourceConfig.getDistributionType().getDistributionStrategy(maxTasks);
		this.inner = Collections.emptyIterator();
		this.outer = Collections.emptyIterator();
	}

	/**
	 * Gets the logger for the concrete implementation.
	 *
	 * @return The logger for the concrete implementation.
	 */

	@Override
	public boolean hasNext() {
		if (!outer.hasNext()) {
			nativeSourceData.recordNativeKeyFinished();
		}
		if (!inner.hasNext() && !outer.hasNext()) {
			inner = nativeSourceData.getIterator(context -> taskId == distributionStrategy.getTaskFor(context));
		}
		while (!outer.hasNext() && inner.hasNext()) {
			outer = nativeSourceData.transform(inner.next()).iterator();
		}
		return outer.hasNext();
	}

	@Override
	public EvolvingSourceRecord next() {
		return outer.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("This iterator is unmodifiable");
	}

}
