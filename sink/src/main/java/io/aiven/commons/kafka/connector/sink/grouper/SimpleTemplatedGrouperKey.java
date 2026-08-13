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

package io.aiven.commons.kafka.connector.sink.grouper;

import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;

/**
 * A simple templated grouperKey that uses the {@link TemplateVariableRegistry#STANDARD_SINK}
 * variable definitions.
 */
public class SimpleTemplatedGrouperKey extends RecordGrouperKey {
  /** The template. */
  private final String templatePattern;

  /**
   * Constructor.
   *
   * @param templatePattern the template to use. May only use variables defined in the {@link
   *     TemplateVariableRegistry#STANDARD_SINK} variable definitions.
   */
  public SimpleTemplatedGrouperKey(final String templatePattern) {
    this.templatePattern = templatePattern;
  }

  @Override
  protected String getTemplatePattern() {
    return templatePattern;
  }
}
