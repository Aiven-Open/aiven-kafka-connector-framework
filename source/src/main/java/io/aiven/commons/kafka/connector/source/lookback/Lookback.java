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
package io.aiven.commons.kafka.connector.source.lookback;

/**
 * Defines the strategy for providing the next key for the client to search for.
 *
 * @param <K> the key type.
 */
public interface Lookback<K extends Comparable<K>> {

  /**
   * Creates a Lookback of the specified size.
   *
   * @param size the size of the lookback.
   * @return a lookback of the specified size;
   * @param <T> the key type.
   */
  static <T extends Comparable<T>> Lookback<T> ofSize(int size) {
    if (size <= 0) {
      return new None<T>();
    }
    if (size == 1) {
      return new LastKey<T>();
    }
    return new Buffer<T>(size);
  }

  /**
   * Adds a key to the lookback tracking.
   *
   * @param key the key to add.
   */
  void add(K key);

  /**
   * Gets the key to query for.
   *
   * @return the key to query for.
   */
  K get();

  /**
   * Determines if the key is in the lookback.
   *
   * @param key the key to search for.
   * @return true if this lookback holds the key.
   */
  boolean contains(K key);

  /**
   * Gets the number of keys this Lookback can store.
   *
   * @return the number of keys this Lookback can store.
   */
  int size();

  /**
   * Resize the lookback. May resize this lookback or create new one. The resulting lookback will
   * contain as much of the latest data from this lookback as possible in the new configuration.
   *
   * @param size the desired size of the lookback. If {@code size < 0} then 0 is used.
   * @return a {@code Lookback} instance of the desired size.
   */
  default Lookback<K> resize(int size) {
    if (size <= 0) {
      if (this instanceof None) {
        return this;
      }
      return new None<>();
    }
    if (size == 1) {
      if (this instanceof LastKey) {
        return this;
      }
      LastKey<K> next = new LastKey<>();
      next.add(this.get());
      return next;
    }
    Buffer<K> next = new Buffer<>(size);
    K key = get();
    if (key != null) {
      next.add(key);
    }
    return next;
  }
}
