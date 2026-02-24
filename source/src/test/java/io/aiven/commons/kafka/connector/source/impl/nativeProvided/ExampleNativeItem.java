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

<<<<<<<< HEAD:source/src/test/java/io/aiven/commons/kafka/connector/source/impl/nativeProvided/ExampleNativeItem.java
package io.aiven.commons.kafka.connector.source.impl.nativeProvided;
========
package io.aiven.commons.kafka.connector.source.impl.test;
>>>>>>>> 566daee (partial  fix):source/src/test/java/io/aiven/commons/kafka/connector/source/impl/test/ExampleNativeItem.java

import java.nio.ByteBuffer;

/**
 * A "native" object for testing. In a real implementation this would be the object returned from
 * storage.
 */
public record ExampleNativeItem(String key, ByteBuffer data) {

  /**
   * Constructor.
   *
   * @param key the key for this object.
   * @param data the data for this object.
   */
  public ExampleNativeItem(final String key, final byte[] data) {
    this(key, ByteBuffer.wrap(data));
  }
}
