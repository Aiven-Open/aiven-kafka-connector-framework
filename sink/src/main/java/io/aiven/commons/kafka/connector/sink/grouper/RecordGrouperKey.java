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
import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import io.aiven.commons.kafka.connector.common.templating.TimestampParser;
import io.aiven.commons.kafka.connector.common.templating.VariableTemplatePart;
import java.text.SimpleDateFormat;
import org.apache.kafka.connect.sink.SinkRecord;

/** The base class for classes that associates {@link SinkRecord}s with groups by some criteria. */
public class RecordGrouperKey {

  /** The parsed template. */
  protected final Template template;

  /**
   * Constructor with Standard Sink grouper registry.
   *
   * @param templatePattern the template pattern to parse.
   */
  public RecordGrouperKey(String templatePattern) {
    this(templatePattern, TemplateVariableRegistry.STANDARD_SINK);
  }

  /**
   * Constructor with Standard Sink grouper registry.
   *
   * @param templatePattern the template pattern to parse.
   * @param registry the TemplateVariableRegistry to use. May be {@code null}.
   */
  public RecordGrouperKey(String templatePattern, TemplateVariableRegistry registry) {
    template = TemplateParser.parse(templatePattern, registry);
  }

  /**
   * Binds the template to the specified record.
   *
   * @param record the record to bind the template to.
   * @return the Bound template.
   */
  protected Template.Bound getBoundTemplate(SinkRecord record) {
    Template.BoundBuilder builder = template.boundBuilder();
    for (String variableName : template.variables()) {
      switch (variableName) {
        case "key":
          builder.bind(variableName, record.key()::toString);
          break;
        case "topic":
          builder.bind(variableName, record::topic);
          break;
        case "partition":
          builder.bind(variableName, record.kafkaPartition()::toString);
          break;
        case "offset":
          builder.bind(variableName, () -> Long.toString(record.kafkaOffset()));
          break;
        case "timestamp":
          VariableTemplatePart vtp = template.variable(variableName).orElse(null);
          if (vtp == null) {
            builder.bind(
                variableName,
                () -> {
                  throw new IllegalStateException("'timestamp' was present and now it is not.");
                });
          } else {
            SimpleDateFormat sdf = TimestampParser.getFormatter(vtp);
            String result = sdf.format(new java.util.Date(record.timestamp()));
            builder.bind(variableName, () -> result);
          }
          break;
        default:
          throw new IllegalArgumentException(variableName + " is an unsupported variable");
      }
    }
    return builder.build();
  }

  /**
   * Creates the group key for the record.
   *
   * @param record the record to generate the key for.
   * @return the key for the record based on the template and template variable registry.
   */
  public String createKey(SinkRecord record) {
    return getBoundTemplate(record).render();
  }
}
