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

import io.aiven.commons.kafka.connector.common.config.validator.BooleanValidator;
import org.apache.kafka.common.config.ConfigDef;

/**
 * The template variable definition.
 */
public final class TemplateVariable {
    /**
     * The standard key definition
     */
    public static final TemplateVariable KEY = new TemplateVariable("key");
    /**
     * the standard topic definition
     */
    public static final TemplateVariable TOPIC = new TemplateVariable("topic");
    /**
     * the standard partition definition
     */
    public static final TemplateVariable PARTITION = new TemplateVariable("partition", new ParameterDescriptor("padding", false, BooleanValidator.INSTANCE));
    /**
     * the standard start offset definition
     */
    public static final TemplateVariable START_OFFSET = new TemplateVariable("start_offset", new ParameterDescriptor("padding", false, BooleanValidator.INSTANCE));
    /**
     * The standard timestamp definition
     */
    public static final TemplateVariable TIMESTAMP = new TemplateVariable("timestamp", new ParameterDescriptor("unit", true, ConfigDef.ValidString.in("yyyy", "MM", "dd", "HH")));

    /**
     * The name of the variable
     */
    private final String name;
    /**
     * The descriptor for the parameter. May be {@code null}.
     */
    private final ParameterDescriptor parameterDescriptor;

    /**
     * Gets the description of this TemplateVariable.
     *
     * @return the description of this template variable.
     */
    @SuppressWarnings({"PMD.CompareObjectsWithEquals", "PMD.ConfusingTernary", "PMD.UselessParentheses"})
    public String description() {
        return (parameterDescriptor != ParameterDescriptor.NO_PARAMETER && parameterDescriptor.hasValidator())
                ? String.join("=", String.join(":", name, parameterDescriptor.getName()), parameterDescriptor.toString())
                : name;
    }

    /**
     * Constructor.
     *
     * @param name the name of the variable.
     */
    TemplateVariable(final String name) {
        this(name, ParameterDescriptor.NO_PARAMETER);
    }

    /**
     * Constructor.
     *
     * @param name                the name of the variable.
     * @param parameterDescriptor the description of the parameter.
     */
    TemplateVariable(final String name, final ParameterDescriptor parameterDescriptor) {
        this.name = name;
        this.parameterDescriptor = parameterDescriptor;
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
     * @return {@code true} if there is a descriptor.
     */
    public boolean hasParameter() {
        return !ParameterDescriptor.NO_PARAMETER.equals(parameterDescriptor);
    }

    public void validate(Parameter parameter) {
        if (hasParameter()) {
            if (parameterDescriptor.isRequired() && parameter == null) {
                throw new IllegalArgumentException(String.format("Variable '%s' parameter '%s' may not be null", name, parameterDescriptor.getName()));
            }
            if (parameter != null) {
                parameterDescriptor.validate(name, parameter.getValue());
            }
        }
    }
}
