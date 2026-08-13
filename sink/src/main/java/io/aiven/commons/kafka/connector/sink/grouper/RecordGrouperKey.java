/*
 * Copyright 2020 Aiven Oy
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
import org.apache.kafka.connect.sink.SinkRecord;

/** The base class for classes that associates {@link SinkRecord}s with groups by some criteria. */
public abstract class RecordGrouperKey {
  /** The parsed template. */
  private Template template;

  /**
   * Get the template pattern. This pattern is used to generate a template using the {@link
   * TemplateParser#parse(String, TemplateVariableRegistry)}
   *
   * @return the template pattern.
   */
  protected abstract String getTemplatePattern();

  /**
   * Gets the template variable registry used during the template parsing. By default, uses the
   * {@link TemplateVariableRegistry#STANDARD_SINK} registry.
   *
   * @return the template variable registry used during the template parsing.
   */
  protected TemplateVariableRegistry getTemplateRegistry() {
    return TemplateVariableRegistry.STANDARD_SINK;
  }

  /**
   * Creates the grouper template from the template pattern and registry.
   *
   * @return the Template for the group key generation.
   */
  protected final synchronized Template grouperTemplate() {
    if (template == null) {
      template = TemplateParser.parse(getTemplatePattern(), getTemplateRegistry());
    }
    return template;
  }

  /**
   * Binds the template to the specified record.
   *
   * @param record the record to bind the template to.
   * @return the Bound template.
   */
  protected Template.Bound getBoundTemplate(SinkRecord record) {
    return grouperTemplate()
        .boundBuilder()
        .bind(TemplateVariable.KEY.getName(), record.key()::toString)
        .bind(TemplateVariable.TOPIC.getName(), record::topic)
        .bind(TemplateVariable.PARTITION.getName(), () -> record.kafkaPartition().toString())
        .bind(TemplateVariable.OFFSET.getName(), () -> Long.toString(record.kafkaOffset()))
        .bind(TemplateVariable.TIMESTAMP.getName(), record.timestamp()::toString)
        .build();
  }

  /**
   * Creates the group key for the record.
   *
   * @param record the record to generate the key for.
   * @return the key for the record based on the template and template variable registry.
   */
  public final String createKey(SinkRecord record) {
    return getBoundTemplate(record).render();
  }
}
