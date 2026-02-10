/*
 * Copyright 2024-2025 Aiven Oy
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.timing.AbortTrigger;
import io.aiven.commons.timing.Backoff;
import io.aiven.commons.timing.BackoffConfig;
import io.aiven.commons.timing.SupplierOfLong;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles extracting records from an iterator and returning them to
 * Kafka. It uses an exponential backoff with jitter to reduce the number of
 * calls to the backend when there is no data. This solution:
 * <ul>
 * <li>When polled this implementation moves available records from the
 * SourceRecord iterator to the return array.</li>
 * <li>if there are no records
 * <ul>
 * <li>{@link #poll()} will return null.</li>
 * <li>The poll will delay no more than approx 5 seconds.</li>
 * </ul>
 * </li>
 * <li>Upto {@link #maxPollRecords} will be sent in a single poll request</li>
 * <li>When the connector is stopped any collected records are returned to kafka
 * before stopping.</li>
 * </ul>
 *
 */
public abstract class AbstractSourceTask<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>> extends SourceTask {

	/**
	 * A {@code null} list representing no values in the polling functionality.
	 */
	public static final List<SourceRecord> NULL_RESULT = null;

	/**
	 * The maximum time to spend polling. This is set to 4 seconds as 5 seconds is
	 * the kafka limit that is allotted to a system for shutdown, and this allows
	 * the polling and iterator to smoothly shutdown accounting for latentcy.
	 */
	public static final Duration MAX_POLL_TIME = Duration.ofSeconds(4);
	/**
	 * The boolean that indicates the connector is stopped.
	 */
	private final AtomicBoolean connectorStopped;

	/**
	 * The logger to use. Set from the class implementing AbstractSourceTask.
	 */
	private Logger logger;

	/**
	 * The maximum number of records to put in a poll. Specified in the
	 * configuration.
	 */
	private int maxPollRecords;

	/**
	 * The transfer queue from concrete implementation to Kafka
	 */
	private LinkedBlockingQueue<SourceRecord> queue;

	/**
	 * The thread that is running the polling of the implementation.
	 */
	private final Thread implemtationPollingThread;

	/**
	 * The Backoff implementation that executes the delay in the poll loop.
	 */
	private final Backoff iteratorBackoff;

	private final Backoff pollBackoff;

	private final BackoffConfig backoffConfig;

	private Iterator<SourceRecord> sourceRecordIterator;

	private AbstractSourceRecordIterator<K, N, O, T> nativeIterator;

	/**
	 * Constructor.
	 */
	protected AbstractSourceTask() {
		super();

		connectorStopped = new AtomicBoolean();
		backoffConfig = new BackoffConfig() {
			@Override
			public SupplierOfLong getSupplierOfTimeRemaining() {
				return MAX_POLL_TIME::toMillis;
			}

			@Override
			public AbortTrigger getAbortTrigger() {
				return () -> {
				};
			}

			@Override
			public boolean applyTimerRule() {
				return false;
			}
		};
		iteratorBackoff = new Backoff(backoffConfig);
		pollBackoff = new Backoff(backoffConfig);
		implemtationPollingThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (stillPolling()) {
						if (!tryAdd()) {
							getLogger().debug("Attempting {}", iteratorBackoff);
							iteratorBackoff.cleanDelay();
						}
					}
				} catch (InterruptedException e) {
					getLogger().warn("{} interrupted -- EXITING", this.toString());
				} catch (RuntimeException e) { // NOPMD AvoidCatchingGenericException
					getLogger().error("{} failed -- EXITING", this.toString(), e);
				} finally {
                    try {
                        nativeIterator.close();
                    } catch (Exception e) {
                        getLogger().error("Error closing iterator: {}", e.getMessage());
                    }
                }
				getLogger().info("{} finished", this.toString());
			}
		}, this.getClass().getName() + " polling thread");
	}

	Logger getLogger() {
		if (logger == null) {
			logger = LoggerFactory.getLogger(this.getClass());
		}
		return logger;
	}

	/**
	 * Gets the iterator of SourceRecords. The iterator that SourceRecords are
	 * extracted from for a poll event. When this iterator runs out of records it
	 * should attempt to reset and read more records from the backend on the next
	 * {@code hasNext()} call. In this way it should detect when new data has been
	 * added to the backend and continue processing.
	 * <p>
	 * This method should handle any backend exception that can be retried. Any
	 * runtime exceptions that are thrown when this iterator executes may cause the
	 * task to abort.
	 * </p>
	 *
	 * @param config the SourceCommonConfig instance.
	 * @return The iterator of SourceRecords.
	 */
	abstract protected AbstractSourceRecordIterator<K, N, O, T> getIterator(SourceCommonConfig config);

	/**
	 * Called by {@link #start} to allows the concrete implementation to configure
	 * itself based on properties.
	 *
	 * @param props
	 *            The properties to use for configuration.
	 *
	 * @return A SourceCommonConfig based configuration.
	 */
	abstract protected SourceCommonConfig configure(Map<String, String> props);

	@Override
	public final void start(final Map<String, String> props) {
		getLogger().debug("Starting");
		final SourceCommonConfig config = configure(props);
		maxPollRecords = config.getMaxPollRecords();
		queue = new LinkedBlockingQueue<>(maxPollRecords * 2);
		nativeIterator = getIterator(config);
		final Iterator<SourceRecord> inner = IteratorUtils.transformedIterator(nativeIterator,
				r -> r.getSourceRecord(config.getErrorsTolerance(), new OffsetManager<>(context)));
		sourceRecordIterator = IteratorUtils.filteredIterator(inner, Objects::nonNull);
		implemtationPollingThread.start();
	}

	/**
	 * Try to add a SourceRecord to the results.
	 *
	 * @return true if successful, false if the iterator is empty.
	 */
	private boolean tryAdd() throws InterruptedException {
		if (queue.remainingCapacity() > 0) {
			if (sourceRecordIterator.hasNext()) {
				iteratorBackoff.reset();
				final SourceRecord sourceRecord = sourceRecordIterator.next();
				if (getLogger().isDebugEnabled()) {
					getLogger().debug("tryAdd() : read record {}", sourceRecord.sourceOffset());
				}
				queue.put(sourceRecord);
				return true;
			}
			getLogger().info("No records found in tryAdd call");
		} else {
			getLogger().info("No space in queue");
		}
		return false;
	}

	/**
	 * Returns {@code true} if the connector is not stopped and the timer has not
	 * expired.
	 *
	 * @return {@code true} if the connector is not stopped and the timer has not
	 *         expired.
	 */
	protected final boolean stillPolling() {
		final boolean result = !connectorStopped.get();
		getLogger().debug("Still polling: {}", result);
		return result;
	}

	@Override
	public final List<SourceRecord> poll() {
		getLogger().debug("Polling");
		if (stillPolling()) {
			List<SourceRecord> results = new ArrayList<>(maxPollRecords);
			results = 0 == queue.drainTo(results, maxPollRecords) ? NULL_RESULT : results;

			if (results == null && !implemtationPollingThread.isAlive()) {
				throw new ConnectException(implemtationPollingThread.getName() + " has died");
			}

			updatePollBackOff(results);

			if (getLogger().isDebugEnabled()) {
				getLogger().debug("Poll() returning {} SourceRecords.", results == null ? null : results.size());
			}

			return results;
		} else {
			getLogger().info("Stopping");

			closeResources();

			return NULL_RESULT;
		}
	}

	private void updatePollBackOff(final List<SourceRecord> results) {
		if (results == NULL_RESULT) {
			pollBackoff.cleanDelay();
		} else {
			pollBackoff.reset();
		}
	}

	@Override
	public final void stop() {
		getLogger().debug("Stopping");
		connectorStopped.set(true);
	}

	/**
	 * Returns the running state of the task.
	 *
	 * @return {@code true} if the connector is running, {@code false} otherwise.
	 */
	public final boolean isRunning() {
		return !connectorStopped.get();
	}

	/**
	 * Close any resources the source has open. Called when stopping.
	 */
	abstract protected void closeResources();

}
