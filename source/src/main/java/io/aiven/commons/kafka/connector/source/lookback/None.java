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
 * Lookback that does not track any keys.
 *
 * @param <K> the key type.
 */
public class None<K extends Comparable<K>> implements Lookback<K> {

  /** Constructor. */
  public None() {}

  @Override
  public void add(K key) {
    // does nothing.
  }

  @Override
  public K get() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean contains(K key) {
    return false;
  }
}
