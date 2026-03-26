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
 * A Lookback that only tracks the last key seen.
 *
 * @param <K> the key type.
 */
public class LastKey<K extends Comparable<K>> implements Lookback<K> {
  private K lastKey;

  /** Constructor. */
  public LastKey() {}

  @Override
  public void add(K key) {
    lastKey = key;
  }

  @Override
  public K get() {
    return lastKey;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public boolean contains(K key) {
    return lastKey != null && key.compareTo(lastKey) == 0;
  }
}
