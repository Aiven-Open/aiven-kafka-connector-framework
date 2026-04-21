/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.util.timing.AbortTrigger;
import io.aiven.commons.util.timing.Backoff;
import io.aiven.commons.util.timing.BackoffConfig;
import io.aiven.commons.util.timing.SupplierOfLong;
import io.aiven.commons.util.timing.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles extracting records from an iterator and returning them to Kafka. It uses an
 * exponential backoff with jitter to reduce the number of calls to the backend when there is no
 * data. This solution:
 *
 * <ul>
 *   <li>When polled this implementation moves available records from the SourceRecord iterator to
 *       the return array.
 *   <li>if there are no records
 *       <ul>
 *         <li>{@link #poll()} will return null.
 *         <li>The poll will delay no more than approx 5 seconds.
 *       </ul>
 *   <li>Upto {@link #maxPollRecords} will be sent in a single poll request
 *   <li>When the connector is stopped any collected records are returned to kafka before stopping.
 * </ul>
 */
public abstract class AbstractSourceTask extends SourceTask {

  /** A {@code null} list representing no values in the polling functionality. */
  public static final List<SourceRecord> NULL_RESULT = null;

  /**
   * The maximum time to spend polling. This is set to 4 seconds as 5 seconds is the kafka limit
   * that is allotted to a system for shutdown, and this allows the polling and iterator to smoothly
   * shutdown accounting for latency.
   */
  public static final Duration MAX_POLL_TIME = Duration.ofSeconds(4);

  /** The boolean that indicates the connector is stopped. */
  private final AtomicBoolean connectorStopped;

  /** The logger to use. Set from the class implementing AbstractSourceTask. */
  private Logger logger;

  /** The maximum number of records to put in a poll. Specified in the configuration. */
  private int maxPollRecords;

  /** The transfer queue from concrete implementation to Kafka */
  private LinkedBlockingQueue<SourceRecord> queue;

  /** The thread that is running the polling of the implementation. */
  private final Thread implemtationPollingThread;

  /** The Backoff implementation that executes the delay in the poll loop. */
  private final Backoff iteratorBackoff;

  private final Backoff pollBackoff;

  private Iterator<SourceRecord> sourceRecordIterator;

  /** Constructor. */
  protected AbstractSourceTask() {
    super();

    connectorStopped = new AtomicBoolean();
    BackoffConfig backoffConfig =
        new BackoffConfig() {
          @Override
          public SupplierOfLong getSupplierOfTimeRemaining() {
            return MAX_POLL_TIME::toMillis;
          }

          @Override
          public AbortTrigger getAbortTrigger() {
            return () -> {};
          }

          @Override
          public boolean applyTimerRule() {
            return false;
          }
        };
    iteratorBackoff = new Backoff(backoffConfig);
    pollBackoff = new Backoff(backoffConfig);
    implemtationPollingThread =
        new Thread(
            new Runnable() {
              @SuppressWarnings("NOPMD.AvoidCatchingGenericException")
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
                  getLogger().warn("{} interrupted -- EXITING", this);
                } catch (RuntimeException e) {
                  getLogger().error("{} failed -- EXITING", this, e);
                }
                getLogger().info("{} finished", this);
              }
            },
            this.getClass().getName() + " polling thread");
  }

  Logger getLogger() {
    if (logger == null) {
      logger = LoggerFactory.getLogger(this.getClass());
    }
    return logger;
  }

  /**
   * Gets the iterator of SourceRecords. The iterator that SourceRecords are extracted from for a
   * poll event. When this iterator runs out of records it should attempt to reset and read more
   * records from the backend on the next {@code hasNext()} call. In this way it should detect when
   * new data has been added to the backend and continue processing.
   *
   * <p>This method should handle any backend exception that can be retried. Any runtime exceptions
   * that are thrown when this iterator executes may cause the task to abort.
   *
   * @param config the SourceCommonConfig instance.
   * @return The iterator of SourceRecords.
   */
  protected abstract EvolvingSourceRecordIterator getIterator(SourceCommonConfig config);

  /**
   * Called by {@link #start} to allows the concrete implementation to configure itself based on
   * properties.
   *
   * @param props The properties to use for configuration.
   * @param offsetManager the OffsetManager to use.
   * @return A SourceCommonConfig based configuration.
   */
  protected abstract SourceCommonConfig configure(
      final Map<String, String> props, final OffsetManager offsetManager);

  @Override
  public final void start(final Map<String, String> props) {
    getLogger().debug("Starting");
    final OffsetManager offsetManager = new OffsetManager(context);
    final SourceCommonConfig config = configure(props, offsetManager);
    maxPollRecords = config.getMaxPollRecords();
    queue = new LinkedBlockingQueue<>(maxPollRecords * 2);
    Iterator<EvolvingSourceRecord> esrIter =
        IteratorUtils.transformedIterator(getIterator(config), this::lastEvolution);
    final Iterator<SourceRecord> inner =
        IteratorUtils.transformedIterator(
            esrIter, r -> r.getSourceRecord(config.getErrorsTolerance(), offsetManager));
    sourceRecordIterator = IteratorUtils.filteredIterator(inner, Objects::nonNull);
    implemtationPollingThread.start();
  }

  /**
   * An insertion point for the last evolution step in the EvolvingSourceRecord. This method is
   * called just before the EvolvingSourceRecord is converted into a SourceRecord. This is the last
   * point to modify
   *
   * <ul>
   *   <li>Key schema and/or value
   *   <li>Value schema and/or value
   *   <li>Offset manager entry
   * </ul>
   *
   * The default implementation makes no changes.
   *
   * @param evolvingSourceRecord the record to modify.
   * @return the modified record.
   */
  protected EvolvingSourceRecord lastEvolution(EvolvingSourceRecord evolvingSourceRecord) {
    return evolvingSourceRecord;
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
   * Returns {@code true} if the connector is not stopped and the timer has not expired.
   *
   * @return {@code true} if the connector is not stopped and the timer has not expired.
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
        getLogger()
            .debug("Poll() returning {} SourceRecords.", results == null ? null : results.size());
        if (results != null) {
          getLogger()
              .debug(
                  results.stream()
                      .map(
                          r ->
                              String.format(
                                  "topic: %s partition: %s key: %s",
                                  r.topic(), r.kafkaPartition(), r.key()))
                      .collect(Collectors.joining("\n")));
        }
      }

      return results;
    } else {
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
    // stop is on a separate thread
    Timer timer = new Timer(Duration.ofSeconds(5));
    timer.start();
    while (implemtationPollingThread.isAlive() && !timer.isExpired()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        getLogger().info("Stopping wait was interrupted: {}", e.getMessage());
      }
    }
    closeResources();
  }

  /**
   * Returns the running state of the task.
   *
   * @return {@code true} if the connector is running, {@code false} otherwise.
   */
  public final boolean isRunning() {
    return !connectorStopped.get();
  }

  /** Close any resources the source has open. Called when stopping. */
  protected abstract void closeResources();
}
