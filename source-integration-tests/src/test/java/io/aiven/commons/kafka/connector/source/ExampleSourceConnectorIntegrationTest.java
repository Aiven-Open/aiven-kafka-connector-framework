/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.commons.kafka.connector.source;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleSourceConnectorIntegrationTest
    extends AbstractSourceConnectorIntegrationTest<String, ByteBuffer> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExampleSourceConnectorIntegrationTest.class);
  private final TestSourceStorage sourceStorage;

  @TempDir static Path testDir;

  ExampleSourceConnectorIntegrationTest() {
    sourceStorage = new TestSourceStorage(testDir);
  }

  @Override
  protected TestConfig getTestConfig() {
    return new ByteTestConfig(sourceStorage);
  }

  @Override
  protected SourceStorage<String, ByteBuffer> getSourceStorage() {
    return sourceStorage;
  }
}
