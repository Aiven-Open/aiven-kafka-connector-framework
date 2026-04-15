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
package io.aiven.commons.kafka.connector.common.documentation;

import io.aiven.commons.kafka.connector.common.templating.TemplateVariable;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import java.util.function.Supplier;

/** Tests for TemplateVariableRegistry */
public class TemplateVariableTestRegistry implements Supplier<TemplateVariableRegistry> {

  TemplateVariableTestRegistry() {}

  /** A registry of the standard variables used in a Sink connector. */
  @Override
  public TemplateVariableRegistry get() {

    return TemplateVariableRegistry.builder()
        .add(TemplateVariable.KEY)
        .add(TemplateVariable.TOPIC)
        .add(TemplateVariable.PARTITION)
        .add(TemplateVariable.START_OFFSET)
        .add(TemplateVariable.TIMESTAMP)
        .build();
  }
}
