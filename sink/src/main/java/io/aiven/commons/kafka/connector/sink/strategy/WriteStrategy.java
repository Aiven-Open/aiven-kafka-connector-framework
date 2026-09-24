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

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/** Defines writing a SinkRecord to storage. */
public interface WriteStrategy {
  /**
   * Add a record to the writer. This can be adding the record to a queue or writing directly to the
   * destination. This method should return as quickly.
   *
   * @param record the record to write.
   */
  void put(SinkRecord record);

  /**
   * A hook for Sink connector to do pre commit processing. See {@link SinkTask#preCommit(Map)} for
   * details.
   *
   * <p>The default here is to call {@link #flush(Map)} and return the {@code currentOffsets}
   * parameter.
   *
   * @param currentOffsets the map of current offsets that may be committed.
   * @return the map of offsets that are committed.
   */
  default Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    flush(currentOffsets);
    return currentOffsets;
  }

  /**
   * flushes data to disk. Will be called if the preCommit is not overridden.
   *
   * @param currentOffsets The offsets to flush.
   */
  void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

  /** Shutdown the wite strategy. */
  void shutdown();
}
