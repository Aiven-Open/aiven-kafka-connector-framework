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
package io.aiven.commons.kafka.connector.source.task;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/** The context for a source record. */
public class Context {
  /** The key for the topic value. */
  public static final String TOPIC_KEY = Context.class.getName() + "#Topic";

  /** the key for the partition value. */
  public static final String PARTITION_KEY = Context.class.getName() + "#Partition";

  /** the key for the offset value. */
  public static final String OFFSET_KEY = Context.class.getName() + "#Offset";

  /** The key for the native key value. */
  public static final String NATIVE_KEY = Context.class.getName() + "#NativeKey";

  /** The properties for this object. */
  private final Map<String, Object> properties;

  /**
   * Create a context from the properties.
   *
   * @param builder The builder to provide the properties.
   */
  public Context(Builder<?> builder) {
    builder.validate();
    this.properties = new LinkedHashMap<>(builder.properties);
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

  @Override
  public String toString() {
    return String.format(
        "ContextImpl{key:%s, partition:%s, topic:%s, offset:%s",
        getNativeKey(), getPartition(), getTopic(), getOffset());
  }

  /**
   * Creates a builder for a Context.
   *
   * @param primaryKey the primary key.
   * @return a builder
   */
  public static Builder<?> builder(Comparable<?> primaryKey) {
    return new DefaultBuilder(primaryKey);
  }

  /**
   * Creates a builder from this context
   *
   * @return a builder for a Context.
   */
  public Context.Builder<?> builder() {
    return new DefaultBuilder(this.properties);
  }

  /**
   * Gets an arbitrary object from the context.
   *
   * @param key the key for the object.
   * @param fn a function to convert the Object into a the desired return type.
   * @return an Optional containing a {@code T} type or an empty optional.
   * @param <T> the object type to return.
   */
  public <T> Optional<T> getObject(String key, Function<Object, T> fn) {
    Object o = properties.get(key);
    return o == null ? Optional.empty() : Optional.of(fn.apply(o));
  }

  /**
   * Gets an optional object from the context.
   *
   * @param key the key to get.
   * @return the optional object or an empty optionl if it was not present.
   */
  public Optional<Object> getObject(String key) {
    return getObject(key, Function.identity());
  }

  /**
   * Gets an optional string from the context.
   *
   * @param key the key to get.
   * @return the optional string or an empty optional if it was not present.
   */
  public Optional<String> getString(String key) {
    return getObject(key, Object::toString);
  }

  /**
   * Gets an optional Number from the context.
   *
   * @param key the key to get.
   * @return the optional Number or an empty optional if it was not present.
   */
  public Optional<Number> getNumber(String key) {
    return getObject(key, Number.class::cast);
  }

  /**
   * Gets an optional Integer from the context.
   *
   * @param key the key to get.
   * @return the optional Integer or an empty optional if it was not present.
   */
  public Optional<Integer> getInteger(String key) {
    return getObject(key, x -> (x instanceof Integer i) ? i : ((Number) x).intValue());
  }

  /**
   * Gets an optional Long from the context.
   *
   * @param key the key to get.
   * @return the optional Long or an empty optional if it was not present.
   */
  public Optional<Long> getLong(String key) {
    return getObject(key, x -> (x instanceof Long l) ? l : ((Number) x).longValue());
  }

  /**
   * Gets an optional Short from the context.
   *
   * @param key the key to get.
   * @return the optional Short or an empty optional if it was not present.
   */
  public Optional<Short> getShort(String key) {
    return getObject(key, x -> (x instanceof Short s) ? s : ((Number) x).shortValue());
  }

  /**
   * Gets the Kafka topic as specified by the context.
   *
   * @return an Optional kafka topic.
   */
  public Optional<String> getTopic() {
    return getString(TOPIC_KEY);
  }

  /**
   * Gets the Kafka partition as specified by the context.
   *
   * @return an Optional kafka partition.
   */
  public Optional<Integer> getPartition() {
    return getInteger(PARTITION_KEY);
  }

  /**
   * Get the native key as specified by this context.
   *
   * @param <T> the returned native key type.
   * @return the Optional storage key for the native object this context is associated with.
   */
  public <T extends Comparable<T>> T getNativeKey() {
    if (properties.get(NATIVE_KEY) instanceof Comparable<?> c) {
      return (T) c;
    }
    if (properties.get(NATIVE_KEY) == null) {
      throw new IllegalStateException("NativeKey may not be null");
    } else {
      throw new IllegalStateException("NativeKey must be an instance of Comparable");
    }
  }

  /**
   * Gets the native offset for the associated data. This may be the number of bytes into the native
   * stream that the associated data starts at, or it may be the number of lines into a text file.
   * The offset definition is dependant upon the native data structure.
   *
   * @return an optional native offset for this context.
   */
  public Optional<Long> getOffset() {
    return getLong(OFFSET_KEY);
  }

  private static final class DefaultBuilder extends Builder<DefaultBuilder> {

    protected DefaultBuilder(Comparable<?> nativeKey) {
      super(nativeKey);
    }

    private DefaultBuilder(Map<String, Object> properties) {
      super(properties);
    }

    public Context build() {
      return new Context(this);
    }
  }

  /**
   * The abstract builder.
   *
   * @param <T> the class of the actual builder.
   */
  public abstract static class Builder<T extends Builder<T>> {

    /** Defines a validation chack for the context properties. */
    @FunctionalInterface
    public interface Validator {
      /**
       * Tests the properties for correctness.
       *
       * @param properties the properties to check.
       * @throws IllegalArgumentException on error.
       */
      void test(Map<String, Object> properties) throws IllegalArgumentException;
    }

    private final Map<String, Object> properties;
    private final List<Validator> validation = new ArrayList<>();

    /**
     * Constructs a builder with the native key.
     *
     * @param nativeKey the native key for the Context.
     */
    protected Builder(Comparable<?> nativeKey) {
      properties = new TreeMap<>();
      properties.put(NATIVE_KEY, nativeKey);
      validation.add(
          properties ->
              Objects.requireNonNull(properties.get(NATIVE_KEY), "Native key may not be null"));
    }

    /**
     * Build the context. Builders should override this method to call the constructor on the
     * desired Context type.
     *
     * @return the new context.
     */
    public abstract Context build();

    /**
     * Constructs a builder with the native key.
     *
     * @param context the context to extract the properties from..
     */
    protected Builder(Context context) {
      this(context.properties);
    }

    /**
     * Constructs a builder from an existing context.
     *
     * @param properties the properties for the context
     */
    protected Builder(Map<String, Object> properties) {
      this.properties = new TreeMap<>(properties);
      validation.add(
          prop -> Objects.requireNonNull(prop.get(NATIVE_KEY), "Native key may not be null"));
    }

    /**
     * Add a validator the the builder check.
     *
     * @param validator the Validator to add.
     */
    protected final void addValidator(Validator validator) {
      validation.add(validator);
    }

    /**
     * Return a reference to this as the class of this builder not the base AbstractBuilder. Used to
     * ensure that additional builder functionality returns the proper type.
     *
     * @return this builder cast to {@code <T>} type.
     */
    public final T self() {
      return (T) this;
    }

    /**
     * Sets the native key.
     *
     * @param nativeKey the native key to use.
     * @return the this.
     */
    public final T nativeKey(Comparable<?> nativeKey) {
      properties.put(NATIVE_KEY, nativeKey);
      return self();
    }

    /**
     * Sets the topic
     *
     * @param topic The topic for the context.
     * @return this
     */
    public final T topic(String topic) {
      properties.put(TOPIC_KEY, topic);
      return self();
    }

    /**
     * Sets the partition.
     *
     * @param partition hhe partition for this context.
     * @return this
     */
    public final T partition(Integer partition) {
      properties.put(PARTITION_KEY, partition);
      return self();
    }

    /**
     * Sets the offset.
     *
     * @param offset the offset for this context.
     * @return this.
     */
    public final T offset(Long offset) {
      properties.put(OFFSET_KEY, offset);
      return self();
    }

    /**
     * Sets an arbitrary property for the context.
     *
     * @param key the key for the property.
     * @param object the property value.
     * @return this.
     */
    public final T set(String key, Object object) {
      if (object == null) {
        properties.remove(key);
      } else {
        properties.put(key, object);
      }
      return self();
    }

    /** Validate that the builder has all the required properties. */
    public final void validate() {
      for (Validator v : validation) {
        v.test(properties);
      }
    }
  }
}
