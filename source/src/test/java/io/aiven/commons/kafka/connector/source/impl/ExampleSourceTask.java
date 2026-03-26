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
package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecordIterator;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.extractor.JsonExtractor;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

public class ExampleSourceTask extends AbstractSourceTask {
  private ExampleNativeSourceData nativeSourceData;

  public ExampleSourceTask() {}

  @Override
  protected EvolvingSourceRecordIterator getIterator(final SourceCommonConfig config) {
    return new EvolvingSourceRecordIterator(config, nativeSourceData);
  }

  @Override
  protected SourceCommonConfig configure(
      final Map<String, String> props, final OffsetManager offsetManager) {
    SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
    setter.extractorClass(JsonExtractor.class);
    SourceCommonConfig result =
        new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    try {
      nativeSourceData = new ExampleNativeSourceData(result, offsetManager);
    } catch (IOException e) {
      throw new ConfigException("Unable to create native source data", e);
    }
    return result;
  }

  @Override
  protected void closeResources() {}

  @Override
  public String version() {
    return "Example version 1.0";
  }
}
