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

package io.aiven.commons.kafka.connector.source.impl.nativeProvided;

import io.aiven.commons.kafka.connector.source.AbstractSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.impl.ExampleNativeSourceData;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * A "native" client. This client returns lists of native objects.
 *
 * <p>In an actual implementation this would connect to the storage and retrieve data.
 */
public class ExampleNativeClient {
  private static Map<String, ExampleNativeItem> dataMap = new TreeMap<>();

  /** Constructor. */
  public ExampleNativeClient() {}

  /** Clear the stored data. */
  public void clear() {
    dataMap.clear();
  }

  /**
   * Write data into the stored data.
   *
   * @param key the key for the data.
   * @param data the data.
   */
  public void write(String key, byte[] data) {
    dataMap.put(key, new ExampleNativeItem(key, data));
  }

  /**
   * List the objects in the stored data.
   *
   * @return the collection of data objects.
   */
  public Collection<ExampleNativeItem> listObjects() {
    return dataMap.values();
  }

  /**
   * Gets a list of native objects. In an actual implementation this method may retrieve one object
   * or may retrieve a number of objects.
   *
   * @param offset This is the key value that was last read or null if there is no last read. It is
   *     a String because the K in the {@link NativeSourceData} as defined in {@link
   *     ExampleNativeSourceData} is a String
   * @return the list of native objects. This is a collection of ExampleNativeItem because that is
   *     the type of the N in {@link AbstractSourceNativeInfo} as defined in {@link
   *     ExampleSourceNativeInfo}.
   */
  public Collection<ExampleNativeItem> listObjects(String offset) {
    System.out.format("Listobject offset %s%n", offset);
    if (offset != null) {
      return dataMap.entrySet().stream()
          .filter(entry -> entry.getKey().compareTo(offset) >= 0)
          .map(Map.Entry::getValue)
          .toList();
    } else {
      return new ArrayList<>(dataMap.values());
    }
  }
}
