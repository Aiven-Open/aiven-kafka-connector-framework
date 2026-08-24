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
import io.aiven.commons.kafka.connector.common.templating.TimestampParser;
import io.aiven.commons.kafka.connector.common.templating.VariableTemplatePart;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.kafka.connect.sink.SinkRecord;

/** The base class for classes that associates {@link SinkRecord}s with groups by some criteria. */
public class RecordGrouperKey {

  private static final TemplateVariableRegistry VARIABLE_REGISTRY;

  /**
   * The map of supported template variable names to a function to extract the string from the
   * Template and SinkRecord.
   */
  public static final Map<String, BiFunction<Template, SinkRecord, Supplier<String>>>
      TEMPLATE_VARIABLE_MAP;

  /** Converts the timestamp based on the definition of the timestamp pattern in the template. */
  private static final BiFunction<Template, SinkRecord, Supplier<String>>
      TIMESTAMP_TEMPLATE_CONVERTER =
          (template, sinkRecord) -> {
            VariableTemplatePart vtp = template.variable(TemplateVariable.TIMESTAMP).orElse(null);
            if (vtp == null) {
              return () -> {
                throw new IllegalStateException("'timestamp' was present and now it is not.");
              };
            } else {
              SimpleDateFormat sdf = TimestampParser.getFormatter(vtp);
              String result = sdf.format(new java.util.Date(sinkRecord.timestamp()));
              return () -> result;
            }
          };

  private static Supplier<String> numberFormatting(
      Template template, TemplateVariable variable, String format, Number number) {

    VariableTemplatePart vtp = template.variable(variable).orElse(null);
    if (vtp == null) {
      return () -> {
        throw new IllegalStateException(
            String.format("'%s' was present and now it is not.", variable.getName()));
      };
    } else {
      return vtp.getParameter().asBoolean()
          ? () -> String.format(format, number)
          : number::toString;
    }
  }

  static {
    VARIABLE_REGISTRY =
        TemplateVariableRegistry.builder()
            .add(TemplateVariableRegistry.STANDARD_SINK)
            .remove(TemplateVariable.OFFSET)
            .remove(TemplateVariable.ORIGINAL_OFFSET)
            .build();

    TEMPLATE_VARIABLE_MAP = new HashMap<>();
    TEMPLATE_VARIABLE_MAP.put(
        TemplateVariable.KEY.getName(),
        (template, sinkRecord) -> () -> sinkRecord.key().toString());
    TEMPLATE_VARIABLE_MAP.put(
        TemplateVariable.TOPIC.getName(), (template, sinkRecord) -> sinkRecord::topic);
    TEMPLATE_VARIABLE_MAP.put(
        TemplateVariable.ORIGINAL_TOPIC.getName(),
        (template, sinkRecord) -> sinkRecord::originalTopic);
    TEMPLATE_VARIABLE_MAP.put(
        TemplateVariable.PARTITION.getName(),
        (template, sinkRecord) ->
            numberFormatting(
                template, TemplateVariable.PARTITION, "%010d", sinkRecord.kafkaPartition()));
    TEMPLATE_VARIABLE_MAP.put(
        TemplateVariable.ORIGINAL_PARTITION.getName(),
        (template, sinkRecord) ->
            numberFormatting(
                template,
                TemplateVariable.ORIGINAL_PARTITION,
                "%010d",
                sinkRecord.originalKafkaPartition()));
    TEMPLATE_VARIABLE_MAP.put(TemplateVariable.TIMESTAMP.getName(), TIMESTAMP_TEMPLATE_CONVERTER);
  }

  /** The parsed template. */
  protected final Template template;

  /**
   * Constructor with Standard Sink grouper registry.
   *
   * @param templatePattern the template pattern to parse.
   */
  public RecordGrouperKey(String templatePattern) {
    this(templatePattern, VARIABLE_REGISTRY);
  }

  /**
   * Constructor with specified template variable registry
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
      BiFunction<Template, SinkRecord, Supplier<String>> converter =
          TEMPLATE_VARIABLE_MAP.get(variableName);
      if (converter == null) {
        throw new IllegalArgumentException(variableName + " is an unsupported variable");
      }
      builder.bind(variableName, converter.apply(template, record));
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
