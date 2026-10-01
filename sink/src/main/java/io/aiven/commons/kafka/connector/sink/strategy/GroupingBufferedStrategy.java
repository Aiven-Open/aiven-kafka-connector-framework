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
package io.aiven.commons.kafka.connector.sink.strategy;

import com.google.common.annotations.VisibleForTesting;
import io.aiven.commons.kafka.connector.common.logging.MdcContextThreadFactory;
import io.aiven.commons.kafka.connector.sink.grouper.RecordGrouperKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * An implementation of write strategy that caches each record into a group based on a {@link
 * RecordGrouperKey}. The caches are flushed whenever Kafka commits the offsets or one of the
 * specified thresholds is met or exceeded.
 */
public class GroupingBufferedStrategy implements WriteStrategy {
  /** Value for thresholds that are not set. */
  public static final int NOT_SET = 0;

  private final long sizeThreshold;
  private final int recordThreshold;
  private final int timeThreshold;
  private final Consumer<List<SinkRecord>> writer;
  private final RecordGrouperKey grouperKey;
  private final Map<String, BufferInfo> buffers = new HashMap<>();
  private final List<Future<?>> queuedWrites = new ArrayList<>();
  private final ExecutorService executor =
      Executors.newCachedThreadPool(new MdcContextThreadFactory());

  /**
   * Constructor.
   *
   * @param grouperKey The grouper key to group records by.
   * @param sizeThreshold the maximum size (in estimated bytes) for the group of records.
   * @param recordThreshold the maximum number of records in a group.
   * @param timeThreshold the maximum age of the oldest record in a group.
   * @param writer the writer to write to storage with.
   */
  public GroupingBufferedStrategy(
      RecordGrouperKey grouperKey,
      long sizeThreshold,
      int recordThreshold,
      int timeThreshold,
      Consumer<List<SinkRecord>> writer) {
    this.sizeThreshold = sizeThreshold;
    this.recordThreshold = recordThreshold;
    this.timeThreshold = timeThreshold;
    this.writer = writer;
    this.grouperKey = grouperKey;
  }

  @Override
  public void put(SinkRecord record) {
    final String recordKey = grouperKey.createKey(record);
    final long recordSize = estimateRecordSize(record);
    buffers.compute(
        recordKey,
        (k, v) -> {
          if (v == null) {
            v = new BufferInfo(recordSize, record);
          } else {
            v.increment(recordSize, record);
            if (isThresholdReached(v)) {
              enqueue(v);
              // remove v from collection
              return null;
            }
          }
          return v;
        });
  }

  @VisibleForTesting
  class TestingInfo {
    boolean isEmpty() {
      return buffers.isEmpty();
    }

    int size() {
      return buffers.size();
    }

    Set<String> keys() {
      return buffers.keySet();
    }

    List<SinkRecord> get(String key) {
      return new ArrayList<>(buffers.get(key).records);
    }
  }

  /**
   * Enqueues the bufferInfo for writing.
   *
   * @param bufferInfo the bufferInfo to write.
   */
  private void enqueue(final BufferInfo bufferInfo) {
    queuedWrites.add(executor.submit(() -> writer.accept(bufferInfo.records)));
  }

  /**
   * Default implementations flushes all records to the write queue and waits for the queue tasks to
   * complete.
   *
   * @param currentOffsets the current offsets to flush.
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    clearTheBuffers();
  }

  void clearTheBuffers() {
    // Enqueue and clear the current buffers.
    buffers.values().forEach(this::enqueue);
    buffers.clear();
    // wait for the writers to complete.
    while (!queuedWrites.isEmpty()) {
      queuedWrites.removeIf(Future::isDone);
    }
  }

  @Override
  public void shutdown() {
    executor.shutdown();
  }

  /**
   * Check if a threshold has been reached for the bufferInfo.
   *
   * @param bufferInfo the bufferinfo to check.
   * @return true if any of the thresholds have been reached.
   */
  protected boolean isThresholdReached(final BufferInfo bufferInfo) {
    return (sizeThreshold != NOT_SET && bufferInfo.bytes >= sizeThreshold)
        || (recordThreshold != NOT_SET && bufferInfo.records.size() >= sizeThreshold)
        || (timeThreshold != NOT_SET && bufferInfo.expires >= System.currentTimeMillis());
  }

  /**
   * Estimates the size of a SinkRecord in bytes. This is a rough approximation based on the byte
   * length of the key and value's String representation.
   *
   * @param record the record to estimate size for.
   * @return the estimated siz.e
   */
  private long estimateRecordSize(final SinkRecord record) {
    long size = 20; // Constant overhead
    if (record.key() != null) {
      size += getObjectSize(record.key());
    }
    if (record.value() != null) {
      size += getObjectSize(record.value());
    }
    return size;
  }

  /**
   * Estimates the size of a SinkRecord object in bytes.
   *
   * @param data the data object to estimate.
   * @return the estimated object size.
   */
  private long getObjectSize(final Object data) {
    if (data instanceof byte[]) {
      return ((byte[]) data).length;
    } else if (data instanceof String) {
      return ((String) data).getBytes(StandardCharsets.UTF_8).length;
    } else {
      return String.valueOf(data).getBytes(StandardCharsets.UTF_8).length;
    }
  }

  /** Tracks each group of buffered records. */
  public class BufferInfo {
    private final long expires;
    private final List<SinkRecord> records;
    private long bytes;

    /**
     * Constructor
     *
     * @param bufferSize the estimated buffer size.
     * @param record the first record.
     */
    BufferInfo(long bufferSize, SinkRecord record) {
      this.bytes = bufferSize;
      this.expires = System.currentTimeMillis() + timeThreshold;
      this.records = new ArrayList<>();
      this.records.add(record);
    }

    /**
     * Adds a record to the buffer info.
     *
     * @param bufferSize the estimated buffer size.
     * @param record the record to add.
     */
    synchronized void increment(long bufferSize, SinkRecord record) {
      this.bytes += bufferSize;
      records.add(record);
    }
  }
}
