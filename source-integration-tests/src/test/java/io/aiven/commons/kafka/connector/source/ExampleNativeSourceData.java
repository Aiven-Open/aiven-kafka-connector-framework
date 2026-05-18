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
package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * An actual NativeSourceData implementation would use a NativeClient to retrieve the NativeItems.
 */
public class ExampleNativeSourceData extends NativeSourceData<String> {
  TestSourceStorage sourceStorage;

  public ExampleNativeSourceData(
      final SourceCommonConfig sourceConfig, final OffsetManager offsetManager) throws IOException {
    super(sourceConfig, offsetManager);
    sourceStorage = new TestSourceStorage(Path.of(sourceConfig.getString("example.dir")));
  }

  @Override
  public String getSourceName() {
    return "Example native source data";
  }

  @Override
  protected Iterator<? extends AbstractSourceNativeInfo<String, ?>> getNativeItemIterator(
      String startFrom) {
    return sourceStorage.getNativeInfo(startFrom).stream()
        .map(ExampleSourceNativeInfo::new)
        .iterator();
  }

  @Override
  public OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Map<String, Object> data) {
    return new ExampleOffsetManagerEntry(data);
  }

  @Override
  protected OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Context context) {
    // cast String because it is the K type in the NativeSourceData<K> above
    return new ExampleOffsetManagerEntry((String) context.getNativeKey());
  }

  @Override
  protected Optional<KeySerde<String>> getNativeKeySerde() {
    return Optional.of(KeySerde.STRING_SERDE);
  }

  @Override
  protected OffsetManager.OffsetManagerKey getOffsetManagerKey(String nativeKey) {
    return sourceStorage.createKey(nativeKey);
  }
}
