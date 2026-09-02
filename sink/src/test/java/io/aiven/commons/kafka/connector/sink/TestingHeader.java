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
package io.aiven.commons.kafka.connector.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

/**
 * An implementation of kafka sink Header for testing.
 *
 * @param key the key value for the header
 * @param schema the schema for the value
 * @param value the value for the header.
 */
public record TestingHeader(String key, Schema schema, Object value) implements Header {
  @Override
  public Header with(Schema schema, Object value) {
    return new TestingHeader(key, schema, value);
  }

  @Override
  public Header rename(String key) {
    return new TestingHeader(key, schema, value);
  }
}
