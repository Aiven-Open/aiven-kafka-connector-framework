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
 *
 *     SPDX-License-Identifier: Apache-2.0
 */

package io.aiven.commons.kafka.connector.source;

import com.google.common.base.Objects;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of OffsetManagerEntry. This entry has 3 values stored in the map.
 *
 * <p>The OffsetManagerEntry must contain a representation of the NativeKey (the K in type in
 * NativeSourceData<K,N,O,T>) The record count must be included but may be set to 1 for all cases
 * where the native source may only return a single Kafka record. All other items are optional.
 */
public class ExampleOffsetManagerEntry
    implements OffsetManager.OffsetManagerEntry, Comparable<ExampleOffsetManagerEntry> {
  public Map<String, Object> data;

  private int recordCount;

  private static final String KEY = "key";
  private static final String RECORD_COUNT = "recordCount";

  /**
   * Constructor.
   *
   * @param nativeKey The native Key.
   */
  public ExampleOffsetManagerEntry(final String nativeKey) {
    this();
    data.put(KEY, nativeKey);
  }

  /** Constructor. */
  private ExampleOffsetManagerEntry() {
    data = new HashMap<>();
  }

  /**
   * A constructor.
   *
   * This constructor is primarily used by the {@link ExampleNativeSourceData} to construct the entry when
   * the OffsetManager determines that the native object has been seen before.
   *
   * @param properties The data map to use.
   * @see <a href="http://aiven-open.github.io/aiven-kafka-connector-framework/source/howto.html">Howto Build A Source Connector</a>
   * for a discussion of OFfsetManagerEntry construction and usage.
   */
  public ExampleOffsetManagerEntry(final Map<String, Object> properties) {
    this();
    data.putAll(properties);
    if (data.containsKey(RECORD_COUNT)) {
      recordCount = getInt(RECORD_COUNT);
    }
  }

  @Override
  public ExampleOffsetManagerEntry fromProperties(final Map<String, Object> properties) {
    return new ExampleOffsetManagerEntry(properties);
  }

  @Override
  public Map<String, Object> getProperties() {
    data.put(RECORD_COUNT, recordCount);
    return data;
  }

  @Override
  public Object getProperty(final String key) {
    return data.get(key);
  }

  @Override
  public void setProperty(final String key, final Object value) {
    data.put(key, value);
  }

  @Override
  public OffsetManager.OffsetManagerKey getManagerKey() {
    // this is the primary key for determining if the data has been processed. At a
    // minimum the representatin of
    // the native key should be stored.
    return () -> Map.of(KEY, data.get(KEY));
  }

  @Override
  public void incrementRecordCount() {
    recordCount++;
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  /**
   * Not part of the standard OffsetManagerEntry. Used in testing to force the system to skip
   * records.
   *
   * @param value the record to start on.
   */
  public void setRecordCount(int value) {
    recordCount = value;
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof ExampleOffsetManagerEntry) {
      return this.compareTo((ExampleOffsetManagerEntry) other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getProperty(KEY));
  }

  @Override
  public int compareTo(final ExampleOffsetManagerEntry other) {
    if (other == this) { // NOPMD
      return 0;
    }
    int result = ((String) getProperty(KEY)).compareTo((String) other.getProperty(KEY));
    if (result == 0) {
      result = Long.compare(getRecordCount(), other.getRecordCount());
    }
    return result;
  }
}
