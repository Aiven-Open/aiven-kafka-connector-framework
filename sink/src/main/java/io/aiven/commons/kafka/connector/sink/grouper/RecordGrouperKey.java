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

import io.aiven.commons.kafka.connector.common.templating.Template;
import io.aiven.commons.kafka.connector.common.templating.TemplateParser;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariable;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import io.aiven.commons.kafka.connector.sink.template.SinkRecordBinding;
import org.apache.kafka.connect.sink.SinkRecord;

/** The base class for classes that associates {@link SinkRecord}s with groups by some criteria. */
public class RecordGrouperKey {

  private static final TemplateVariableRegistry VARIABLE_REGISTRY;

  static {
    VARIABLE_REGISTRY =
        TemplateVariableRegistry.builder()
            .add(TemplateVariableRegistry.STANDARD_SINK)
            .remove(TemplateVariable.OFFSET)
            .remove(TemplateVariable.ORIGINAL_OFFSET)
            .build();
  }

  /** The parsed template. */
  protected final Template template;

  /**
   * Constructor with Standard Sink grouper registry.
   *
   * @param templatePattern the template pattern to parse.
   */
  public RecordGrouperKey(final String templatePattern) {
    this(templatePattern, VARIABLE_REGISTRY);
  }

  /**
   * Constructor with specified template variable registry
   *
   * @param templatePattern the template pattern to parse.
   * @param registry the TemplateVariableRegistry to use. May be {@code null}.
   */
  public RecordGrouperKey(final String templatePattern, final TemplateVariableRegistry registry) {
    template = TemplateParser.parse(templatePattern, registry);
  }

  /**
   * Binds the template to the specified record.
   *
   * @param record the record to bind the template to.
   * @return the Bound template.
   */
  protected Template.Bound getBoundTemplate(final SinkRecord record) {
    return SinkRecordBinding.bind(template.boundBuilder(), record).build();
  }

  /**
   * Creates the group key for the record.
   *
   * @param record the record to generate the key for.
   * @return the key for the record based on the template and template variable registry.
   */
  public String createKey(final SinkRecord record) {
    return getBoundTemplate(record).render();
  }

  /**
   * Determines if the variable is in the key.
   *
   * @param variable the variable to search for.
   * @return {@code true} if the variable is in the key, {@code false} otherwise.
   */
  public boolean hasVariable(final TemplateVariable variable) {
    return hasVariable(variable.getName());
  }

  /**
   * Determines if the variable is in the key.
   *
   * @param name the name of variable to search for.
   * @return {@code true} if the variable is in the key, {@code false} otherwise.
   */
  public boolean hasVariable(final String name) {
    return template.variables().contains(name);
  }
}
