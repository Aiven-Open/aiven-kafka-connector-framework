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

package io.aiven.commons.kafka.connector.source.task;

import java.util.Optional;

/**
 * A Context which captures all the details about the source object that are required to
 * successfully send a source record onto Kafka
 */
public class Context {
  /** The Kafka topic for this Context. May be {@code null}. */
  private String topic;

  /** The Kafka partition for this Context. May be {@code null}. */
  private Integer partition;

  /**
   * The Kafka offset for this Context. When used as a Context within a larger context, this is the
   * number of bytes into the native stream that this context starts at. May be {@code null}.
   */
  private Long offset;

  /** the native key that is being processed */
  private final Comparable<?> nativeKey;

  /**
   * Constructor.
   *
   * @param nativeKey The native key for the object being processed.
   */
  public Context(Comparable<?> nativeKey) {
    this.nativeKey = nativeKey;
  }

  /**
   * Creates a defensive copy of the Context
   *
   * @param anotherContext The Context which needs to be copied
   */
  protected Context(final Context anotherContext) {
    this.nativeKey = anotherContext.nativeKey;
    this.partition = anotherContext.partition;
    this.topic = anotherContext.topic;
    this.offset = anotherContext.offset;
  }

  @Override
  public String toString() {
    return String.format(
        "Context{key:%s, partition:%s, topic:%s, offset:%s", nativeKey, partition, topic, offset);
  }

  /**
   * Gets the Kafka topic as specified by the context.
   *
   * @return an Optional kafka topic.
   */
  public final Optional<String> getTopic() {
    return Optional.ofNullable(topic);
  }

  /**
   * Sets the Kafka topic for this context.
   *
   * @param topic the topic. May be {@code null}.
   */
  public final void setTopic(final String topic) {
    this.topic = topic;
  }

  /**
   * Gets the Kafka partition as specified by the context.
   *
   * @return an Optional kafka partition.
   */
  public final Optional<Integer> getPartition() {
    return Optional.ofNullable(partition);
  }

  /**
   * Sets the Kafka partition for this context.
   *
   * @param partition the partition. May be {@code null}.
   */
  public final void setPartition(final Integer partition) {
    this.partition = partition;
  }

  /**
   * Get the native key as specified by this context.
   *
   * @param <T> the returned native key type.
   * @return the Optional storage key for the native object this context is associated with.
   */
  public final <T extends Comparable<T>> T getNativeKey() {
    return (T) nativeKey;
  }

  /**
   * Gets the native offset for this context. When used as a Context within a larger context, this
   * is the number of bytes into the native stream that this context starts at.
   *
   * @return an optional native offset for this context.
   */
  public final Optional<Long> getOffset() {
    return Optional.ofNullable(offset);
  }

  /**
   * Sets the native offset for this context. When used as a Context within a larger context, this
   * is the number of bytes into the native stream that this context starts at.
   *
   * @param offset the optional native offset for this context. May be {@code null}.
   */
  public final void setOffset(final Long offset) {
    this.offset = offset;
  }
}
