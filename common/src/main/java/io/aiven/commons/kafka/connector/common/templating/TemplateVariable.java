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
package io.aiven.commons.kafka.connector.common.templating;

import java.util.Objects;
import org.apache.commons.text.WordUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** The template variable definition. */
public final class TemplateVariable {
  // start of static standard template variables
  /** The padding descriptor used below */
  private static final ParameterDescriptor PADDING_DESCRIPTOR =
      ParameterDescriptor.builder("padding")
          .validator(ConfigDef.ValidString.in("true", "false"))
          .description("Specifies that the value should be left padded")
          .build();

  /** The standard key definition */
  public static final TemplateVariable KEY =
      builder("key").description("The key from the Kafka source record").build();

  /** the standard topic definition */
  public static final TemplateVariable TOPIC =
      builder("topic").description("The topic the message was read from").build();

  /** the standard partition definition */
  public static final TemplateVariable PARTITION =
      builder("partition")
          .parameterDescriptor(PADDING_DESCRIPTOR)
          .description("The partition the message was read from")
          .build();

  /** the standard start offset definition */
  public static final TemplateVariable START_OFFSET =
      builder("start_offset").parameterDescriptor(PADDING_DESCRIPTOR).build();

  /** The standard timestamp definition */
  public static final TemplateVariable TIMESTAMP =
      builder("timestamp")
          .parameterDescriptor(
              ParameterDescriptor.builder("unit")
                  .required(true)
                  .validator(ConfigDef.ValidString.in("yyyy", "MM", "dd", "HH"))
                  .description("Specifies the format of the timestamp."))
          .description("The timestamp from the message")
          .build();

  /** The name of the variable */
  private final String name;

  /** The descriptor for the parameter. May be {@code null}. */
  private final ParameterDescriptor parameterDescriptor;

  /** The optional description of this template variable */
  private final String description;

  private TemplateVariable(Builder builder) {
    this.name = builder.name;
    this.parameterDescriptor = builder.parameterDescriptor;
    this.description = builder.description;
  }

  /**
   * Gets the description of this TemplateVariable.
   *
   * @return the description of this template variable.
   */
  public String description() {
    return description != null
        ? description
        : String.format("%s%s", name, hasParameter() ? parameterDescriptor.example() : "");
  }

  /**
   * Generates an example of this variable.
   *
   * @return the example text for this variable.
   */
  public String example() {
    return String.format(
        "{{ %s%s }}", name, hasParameter() ? ":" + parameterDescriptor.example() : "");
  }

  /**
   * Gets the builder for TemplateVariables.
   *
   * @param name name of the variable being built.
   * @return the Builder.
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Gets the name of the variable.
   *
   * @return the name of the variable.
   */
  public String getName() {
    return name;
  }

  /**
   * Determines if there is a parameter descriptor.
   *
   * @return {@code true} if there is a descriptor.
   */
  public boolean hasParameter() {
    return !ParameterDescriptor.NO_PARAMETER.equals(parameterDescriptor);
  }

  /**
   * Gets the parameter descriptor for this variable.
   *
   * @return the parameter descriptor.
   */
  public ParameterDescriptor getParameterDescriptor() {
    return parameterDescriptor;
  }

  /**
   * Validate the parameter against the template variable.
   *
   * @param errStr the error string for this template.
   * @param template the template the parameter was parsed from.
   * @param parameter The parsed parameter
   */
  public void validate(String errStr, String template, Parameter parameter) {
    Objects.requireNonNull(errStr, "errStr must not be null");
    Objects.requireNonNull(template, "template must not be null");
    Objects.requireNonNull(parameter, "parsed parameter must not be null");
    if (hasParameter()) {
      if (parameterDescriptor.isRequired() && parameter.equals(Parameter.EMPTY)) {
        String errMsg =
            String.format("parameter '%s' must be specified", parameterDescriptor.getName());
        if (parameterDescriptor.hasValidator()) {
          String[] parts = parameterDescriptor.getValidatorHelp().split(" ");
          parts[0] = " and";
          parts[1] = WordUtils.uncapitalize(parts[1]);
          errMsg += String.join(" ", parts);
        }
        throw new ConfigException(errStr, template, errMsg);
      }
      if (!parameter.equals(Parameter.EMPTY)) {
        if (!parameterDescriptor.getName().equals(parameter.getName())) {
          throw new ConfigException(
              errStr,
              template,
              String.format("parameter name should be '%s'", parameterDescriptor.getName()));
        }
        parameterDescriptor.validate(
            String.format("%s parameter '%s'", errStr, parameterDescriptor.getName()),
            template,
            parameter.getValue());
      }
    }
  }

  /** A builder for TemplateVariable instances. */
  public static class Builder {
    private final String name;
    private ParameterDescriptor parameterDescriptor;
    private String description;

    private Builder(String name) {
      this.name = name;
      this.parameterDescriptor = ParameterDescriptor.NO_PARAMETER;
      if (TemplateParser.INVALID_NAME.matcher(name).find()) {
        throw new IllegalArgumentException(
            String.format("TemplateVariable names may not contain whitespace: '%s", name));
      }
    }

    /**
     * Sets the description for the variable.
     *
     * @param description the description.
     * @return this
     */
    public Builder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the parameter descriptor for this variable.
     *
     * @param descriptor the parameter descriptor.
     * @return this
     */
    public Builder parameterDescriptor(ParameterDescriptor descriptor) {
      this.parameterDescriptor = descriptor;
      return this;
    }

    /**
     * Sets the parameter descriptor for this variable.
     *
     * @param builder the parameter descriptor builder.
     * @return this
     */
    public Builder parameterDescriptor(ParameterDescriptor.Builder builder) {
      this.parameterDescriptor = builder.build();
      return this;
    }

    /**
     * Creates a TemplateVariable.
     *
     * @return a new template variable.
     */
    public TemplateVariable build() {
      return new TemplateVariable(this);
    }
  }
}
