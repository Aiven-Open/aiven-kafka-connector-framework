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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Objects;

/**
 * The Description of a parameter including the name and an input validator.
 */
public final class ParameterDescriptor {

    /**
     * The NO PARAMETER instance.
     */
    public static final ParameterDescriptor NO_PARAMETER = ParameterDescriptor.builder("__no_parameter__").build();

    /**The name of the parameter  */
    private final String name;

    /** The  required flag */
    private final boolean required;

    /** The validator for the input */
    private final ConfigDef.Validator validator;

    /**
     * Constructor.
     * @param name the name of the parameter
     * @param required the required flag.
     * @param values the valid values
     * @deprecated use {@link #builder(String)} and fluent builder.
     */
    @Deprecated
    public ParameterDescriptor(final String name, final boolean required, final List<String> values) {
        this(builder(name).required(required).validator(values.isEmpty() ? null : ConfigDef.ValidString.in(values.toArray(new String[0]))));
    }

    private ParameterDescriptor(Builder builder) {
        this.name = builder.name;
        this.required = builder.required;
        this.validator = builder.validator;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * Gets the parameter name.
     * @return the parameter name.
     */
    public String getName() {
        return name;
    }

    /**
     * Determines if a validator is available.
     * @return {@code true} if a validator is available.
     */
    public boolean hasValidator() {
        return validator != null;
    }

    /**
     * Gets the validator help text.
     * @return the validator help text or an empty string if no validator is available.
     */
    public String getValidatorHelp() {
        if (hasValidator()) {
            try {
                validator.ensureValid("---MARKER---", null);
                return validator.toString();
            } catch (ConfigException e) {
                return e.getMessage().split("---MARKER---:")[1];
            }
        }
        return "";
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

    public void validate(String variable, String template, String value) {
        if (hasValidator()) {
            validator.ensureValid(String.format("%s in '%s'", variable, template), value);
        }
    }

    public static class Builder {
        /**The name of the parameter  */
        private final String name;

        /** The  required flag */
        private boolean required;

        /** The validator for the input */
        private ConfigDef.Validator validator;

        private String description;

        private Builder(String name) {
            this.name = name;
        }

        public Builder required(boolean state) {
            required = state;
            return this;
        }

        public Builder validator(ConfigDef.Validator validator) {
            this.validator = validator;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

       public ParameterDescriptor build() {
            return new ParameterDescriptor(this);
       }
    }
}
