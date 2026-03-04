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

import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Objects;

/**
 * The Description of a parameter including the name and an input validator.
 */
public final class ParameterDescriptor {

    /**
     * The NO PARAMETER instance.
     */
    public static final ParameterDescriptor NO_PARAMETER = new ParameterDescriptor("__no_parameter__", false, (ConfigDef.Validator)null);

    /**The name of the parameter  */
    private final String name;

    /** The  required flag */
    private final boolean required;

    /** The validator for the input */
    private final ConfigDef.Validator validator;

    @Deprecated
    public ParameterDescriptor(final String name, final boolean required, final List<String> values) {
        this(name, required, values.isEmpty() ? null : ConfigDef.ValidString.in(values.toArray(new String[0])));
    }

    /**
     * Constructor.
     * @param name the name of the parameter.
     * @param required {@code true} if the parameter is requried.
     * @param validator The validator for the data.
     */
    public ParameterDescriptor(final String name, final boolean required, final ConfigDef.Validator validator) {
        this.name = name;
        this.required = required;
        this.validator = validator;
    }

    /**
     * Gets the parameter name.
     * @return the parameter name.
     */
    public String getName() {
        return name;
    }

    public boolean hasValidator() {
        return validator != null;
    }

    /**
     * Gets the required flag.
     * @return {@code true} if the parameter is required.
     */
    public boolean isRequired() {
        return required;
    }

    /**
     * Gets the validator description.
     * @return the validator description.
     */
    @Override
    public String toString() {
        return hasValidator() ? validator.toString() : "no validator";
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ParameterDescriptor that)) {
            return false;
        }
        return required == that.required && Objects.equals(name, that.name) && Objects.equals(validator, that.validator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, required);
    }

    public void validate(String variable, String value) {
        if (hasValidator()) {
            validator.ensureValid(String.format("variable '%s' parameter '%s'", variable, name), value);
        }
    }
}
