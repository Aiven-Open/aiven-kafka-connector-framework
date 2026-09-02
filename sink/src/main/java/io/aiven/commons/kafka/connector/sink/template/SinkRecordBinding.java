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
package io.aiven.commons.kafka.connector.sink.template;

import io.aiven.commons.kafka.connector.common.templating.Parameter;
import io.aiven.commons.kafka.connector.common.templating.Template;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariable;
import io.aiven.commons.kafka.connector.common.templating.TimestampParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/** Performs standard bindings for the standard sink variables */
public class SinkRecordBinding {

  /**
   * The map of supported template variable names to a function to extract the string from the
   * Template and SinkRecord.
   */
  private static final Map<String, TemplateVariable> TEMPLATE_VARIABLES;

  static {
    TEMPLATE_VARIABLES = new HashMap<>();
    for (TemplateVariable var :
        List.of(
            TemplateVariable.KEY,
            TemplateVariable.TOPIC,
            TemplateVariable.ORIGINAL_TOPIC,
            TemplateVariable.PARTITION,
            TemplateVariable.ORIGINAL_PARTITION,
            TemplateVariable.TIMESTAMP,
            TemplateVariable.OFFSET,
            TemplateVariable.ORIGINAL_OFFSET,
            TemplateVariable.TIMESTAMP)) {
      TEMPLATE_VARIABLES.put(var.getName(), var);
    }
  }

  private SinkRecordBinding() {
    // do not instantiate
  }

  /**
   * Formats numbers with leading zeros as necessary.
   *
   * @param parameter the parameter for the template variable.
   * @param format the desired format.
   * @param number the number that is to be formatted.
   * @return a supplier of string.
   */
  private static String numberFormatting(Parameter parameter, String format, Number number) {
    return parameter.asBoolean() ? String.format(format, number) : number.toString();
  }

  private static String timestampFormatting(Parameter parameter, long timestamp) {
    return parameter.getValue() == null
        ? Long.toString(timestamp)
        : TimestampParser.getFormatter(parameter.getValue()).format(new java.util.Date(timestamp));
  }

  /**
   * bind the template builder to the record. Any existing sink records that are in the template
   * wil;l be reset.
   *
   * @param builder the Builder to bind.
   * @param record the record to bind to.
   * @return the bound builder.
   */
  public static Template.BoundBuilder bind(Template.BoundBuilder builder, final SinkRecord record) {
    for (String name : builder.getVariableNames()) {
      TemplateVariable var = TEMPLATE_VARIABLES.get(name);
      if (var != null) {
        if (var.equals(TemplateVariable.KEY)) {
          builder.bind(TemplateVariable.KEY, record.key().toString());
        } else if (var.equals(TemplateVariable.TOPIC)) {
          builder.bind(TemplateVariable.TOPIC, record.topic());
        } else if (var.equals(TemplateVariable.ORIGINAL_TOPIC)) {
          builder.bind(TemplateVariable.ORIGINAL_TOPIC, record.originalTopic());
        } else if (var.equals(TemplateVariable.PARTITION)) {
          builder.bind(
              TemplateVariable.PARTITION,
              parameter -> numberFormatting(parameter, "%010d", record.kafkaPartition()));
        } else if (var.equals(TemplateVariable.ORIGINAL_PARTITION)) {
          builder.bind(
              TemplateVariable.ORIGINAL_PARTITION,
              parameter -> numberFormatting(parameter, "%010d", record.originalKafkaPartition()));
        } else if (var.equals(TemplateVariable.OFFSET)) {
          builder.bind(
              TemplateVariable.OFFSET,
              parameter -> numberFormatting(parameter, "%020d", record.kafkaOffset()));
        } else if (var.equals(TemplateVariable.ORIGINAL_OFFSET)) {
          builder.bind(
              TemplateVariable.ORIGINAL_OFFSET,
              parameter -> numberFormatting(parameter, "%020d", record.originalKafkaOffset()));
        } else if (var.equals(TemplateVariable.TIMESTAMP)) {
          builder.bind(
              TemplateVariable.TIMESTAMP,
              parameter -> timestampFormatting(parameter, record.timestamp()));
        }
      }
    }
    return builder;
  }
}
