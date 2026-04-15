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
 *        SPDX-License-Identifier: Apache-2.0
 */

package io.aiven.commons.kafka.connector.source.impl.csv;

import io.aiven.commons.kafka.connector.source.impl.nativeProvided.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.testFixture.format.CsvTestDataFixture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A "native" client. This client returns lists of native objects and ByteBuffers for specific
 * native keys.
 *
 * <p>In an actual implementation this would connect to the storage and retrieve data.
 */
public class ExampleCsvClient {
  public final Map<String, String> dataMap;
  public String dataSent;

  public ExampleCsvClient() throws IOException {
    dataMap = new LinkedHashMap<>();
    dataMap.put("first 10", CsvTestDataFixture.generateCsvRecords(10));
    dataMap.put(
        "second 10",
        CsvTestDataFixture.generateCsvRecords(10, 10, CsvTestDataFixture.TEST_MESSAGE));
    dataMap.put(
        "third 10", CsvTestDataFixture.generateCsvRecords(20, 10, CsvTestDataFixture.TEST_MESSAGE));
  }

  /**
   * Gets a list of native objects. In an actual implementation this method may retrieve one object
   * or may retrieve a number of objects.
   *
   * @param offset This is the key value that was last read or null if there is no last read. It is
   *     a String because the K in the {@code NativeSourceData<K,N,O,T>} as defined in {@link
   *     ExampleCsvSourceData} is a String
   * @return the list of native objects. This is a collection of String because that is the type of
   *     the N in {@code NativeSourceData<K,N,O,T>}
   */
  Collection<ExampleNativeItem> listObjects(String offset) {
    /*
     * In real life the calling framework tracks what has been processed via the
     * OffsetManager so sending the same data multiple times in not an issue.
     * However, in this simple version we are not tracking the offsets so we only
     * send the block of data once. This example simulates returning 2 items, and
     * after they are processed finding the third. After the third item is processed
     * there are no more data elements.
     */
    if (dataSent == null) {
      dataSent = "second 10";
      return Arrays.asList(
          new ExampleNativeItem(
              dataMap.get("first 10"), dataMap.get("first 10").getBytes(StandardCharsets.UTF_8)),
          new ExampleNativeItem(
              dataMap.get("second 10"), dataMap.get("second 10").getBytes(StandardCharsets.UTF_8)));
    } else if (dataSent.equals("second 10")) {
      dataSent = "third 10";
      return Collections.singletonList(
          new ExampleNativeItem(
              dataMap.get("third 10"), dataMap.get("third 10").getBytes(StandardCharsets.UTF_8)));
    }
    return Collections.emptyList();
  }

  /**
   * Gets the ByteBuffer for a key.
   *
   * @param key the key to get the byte buffer for.
   * @return The ByteBuffer for a key, or {@code null} if not set.
   */
  ByteBuffer getObjectAsBytes(String key) {
    String nativeItem = dataMap.get(key);
    return nativeItem == null ? null : ByteBuffer.wrap(nativeItem.getBytes(StandardCharsets.UTF_8));
  }
  ;
}
