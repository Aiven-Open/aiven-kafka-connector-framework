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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * A Context which captures all the details about the source object that are required to
 * successfully send a source record onto Kafka
 */
public final class Context {
  public static final String TOPIC_KEY = Context.class.getName()+"#Topic";
  public static final String PARTITION_KEY = Context.class.getName()+"#Partition";
  public static final String OFFSET_KEY = Context.class.getName()+"#Offset";
  public static final String NATIVE_KEY = Context.class.getName()+"#NativeKey";

  final Map<String,Object> properties;

//  /** The Kafka topic for this Context. May be {@code null}. */
//  private String topic;
//
//  /** The Kafka partition for this Context. May be {@code null}. */
//  private Integer partition;
//
//  /**
//   * The Kafka offset for this Context. When used as a Context within a larger context, this is the
//   * number of bytes into the native stream that this context starts at. May be {@code null}.
//   */
//  private Long offset;
//
//  /** the native key that is being processed */
//  private final Comparable<?> nativeKey;

  public static Builder builder(Comparable<?> primaryKey) {
    return new Builder(primaryKey);
  }

  public static Builder builder(Context context) {
    return new Builder(context);
  }

  /**
   * Constructor.
   *
   * @param properties The map of properties for this context.
   */
  private Context(Map<String, Object> properties) {
    this.properties = new TreeMap<>(properties);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Context context = (Context) o;
    return Objects.equals(properties, context.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(properties);
  }

  public Builder builder() {
    return new Builder(this);
  }

  @Override
  public String toString() {
    return String.format(
        "Context{key:%s, partition:%s, topic:%s, offset:%s", getNativeKey(), getPartition(), getTopic(), getOffset());
  }

  private <T> Optional<T> getObject(String key, Function<Object, T> fn) {
    Object o = properties.get(key);
    return o == null ? Optional.empty() : Optional.of(fn.apply(o));
  }

  public final Optional<String> getString(String key) {
    return getObject(key, Object::toString);
  }

  public Optional<Number> getNumber(String key) {
    return getObject(key, Number.class::cast);
  }

  public Optional<Integer> getInteger(String key) {
    return getObject(key,  x -> (x instanceof Integer i) ? i : ((Number) x).intValue());
  }

  public Optional<Long> getLong(String key) {
    return getObject(key,  x -> (x instanceof Long l) ? l : ((Number) x).longValue());
  }

  public Optional<Short> getShort(String key) {
    return getObject(key,  x -> (x instanceof Short s) ? s : ((Number) x).shortValue());
  }


  /**
   * Gets the Kafka topic as specified by the context.
   *
   * @return an Optional kafka topic.
   */
  public final Optional<String> getTopic() {
    return getString(TOPIC_KEY);
  }

  /**
   * Gets the Kafka partition as specified by the context.
   *
   * @return an Optional kafka partition.
   */
  public final Optional<Integer> getPartition() {
    return getInteger(PARTITION_KEY);
  }

  /**
   * Get the native key as specified by this context.
   *
   * @param <T> the returned native key type.
   * @return the Optional storage key for the native object this context is associated with.
   */
  public final <T extends Comparable<T>> T getNativeKey() {
    return (T) properties.get(NATIVE_KEY);
  }

  /**
   * Gets the native offset for this context. When used as a Context within a larger context, this
   * is the number of bytes into the native stream that this context starts at.
   *
   * @return an optional native offset for this context.
   */
  public final Optional<Long> getOffset() {
    return getLong(OFFSET_KEY);
  }



  public static class AbstractBuilder<T extends AbstractBuilder<T>> {
    private final Map<String, Object> properties;

    protected AbstractBuilder(Comparable<?> nativeKey) {
      properties = new TreeMap<>();
      properties.put(NATIVE_KEY, nativeKey);
    }

    protected AbstractBuilder(Context otherContext) {
      properties = new TreeMap<>(otherContext.properties);
    }

    public final T self() {
      return (T) this;
    }

    public final T nativeKey(Comparable<?> nativeKey) {
      properties.put(NATIVE_KEY, nativeKey);
      return self();
    }

    public final T topic(String topic) {
      properties.put(TOPIC_KEY, topic);
      return self();
    }

    public final T partition(Integer partition) {
      properties.put(PARTITION_KEY, partition);
      return self();
    }

    public final T offset(Long offset) {
      properties.put(OFFSET_KEY, offset);
      return self();
    }

    public final Context build() {
      Objects.requireNonNull(properties.get(NATIVE_KEY), "Native key may not be null");
      return new Context(properties);
    }
  }

  public static class Builder extends AbstractBuilder<Builder> {

    public Builder(Comparable<?> nativeKey) {
      super(nativeKey);
    }

    public Builder(Context otherContext) {
      super(otherContext);
    }
  }
}
