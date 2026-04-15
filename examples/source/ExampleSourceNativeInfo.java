/*
 * Copyright 2024-2025 Aiven Oy
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

package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.AbstractSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.impl.nativeProvided.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/** This example data object has a String key and a byte buffer as its data object. */
public class ExampleSourceNativeInfo extends AbstractSourceNativeInfo<String, ByteBuffer> {

  /**
   * Constructor.
   *
   * @param item the ExampleNativeItem comprising a key of String and a data item of ByteBuffer.
   */
  public ExampleSourceNativeInfo(ExampleNativeItem item) {
    super(new NativeInfo<String, ByteBuffer>(item.key(), item.data()));
    // in some cases it would make sense to pass the ExampleNativeItem as the data
    // type and extract the key type from that
  }

  @Override
  public Context getContext() {
    return new Context(nativeKey());
    // we don't have any context data here, but in some cases the ExampleNativeItem
    // may have topic, partition or kafka offset information.
  }

  @Override
  protected InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream(nativeInfo.nativeItem().array());
  }

  @Override
  public long estimateInputStreamLength() throws UnsupportedOperationException {
    return nativeInfo.nativeItem().capacity();
  }
}
