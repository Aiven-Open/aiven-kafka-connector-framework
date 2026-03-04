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
package io.aiven.commons.kafka.connector.common.config.validator;

import io.aiven.commons.kafka.connector.common.templating.ParameterDescriptor;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariable;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatException;

/**
 * Tests for the template validator
 */
public class TemplateValidatorTest {
    
    public static final TemplateVariableRegistry TESTING_REGISTRY = TemplateVariableRegistry.builder().add(
                    TemplateVariable.KEY).add(TemplateVariable.PARTITION).add(TemplateVariable.TIMESTAMP)
            .build();

    final TemplateValidator underTest = new TemplateValidator(TESTING_REGISTRY);
    @Test
    void missingKey() {
        assertThatException().isThrownBy(() ->underTest.ensureValid("CONFIGURATION_NAME", "{{key}}-{{missing}}"))
                .isInstanceOf(ConfigException.class).withMessage(
        "Invalid value {{key}}-{{missing}} for configuration CONFIGURATION_NAME template variable 'missing': 'missing' is not defined in the variable registry");
    }
    
    @Test
    void wrongParameterName() {
        assertThatException().isThrownBy(() ->underTest.ensureValid("CONFIGURATION_NAME", "{{partition:notPadding=true}}"))
                .isInstanceOf(ConfigException.class).withMessage(
                        "Invalid value {{partition:notPadding=true}} for configuration CONFIGURATION_NAME template variable 'partition': parameter name should be 'padding'");
    }

    @Test
    void wrongParameterValue() {
        assertThatException().isThrownBy(() ->underTest.ensureValid("CONFIGURATION_NAME", "{{partition:padding=notABoolean}}"))
                .isInstanceOf(ConfigException.class).withMessage(
                        "Invalid value notABoolean for configuration CONFIGURATION_NAME template variable 'partition' parameter 'padding' in '{{partition:padding=notABoolean}}': String must be one of (case insensitive): TRUE, FALSE");
    }

    @Test
    void missingParameterValue() {
        assertThatException().isThrownBy(() ->underTest.ensureValid("CONFIGURATION_NAME", "{{partition:padding=}}"))
                .isInstanceOf(ConfigException.class).withMessage(
                        "Invalid value parameter `padding` value has not been set for configuration CONFIGURATION_NAME template variable 'partition': {{partition:padding=}}");
    }

    @Test
    void optionalParameterNotSpecified() {
        // should validate with no issues
        underTest.ensureValid("CONFIGURATION_NAME", "{{partition}}");
    }
    
    @Test
    void missingRequiredParameter() {
        assertThatException().isThrownBy(() ->underTest.ensureValid("CONFIGURATION_NAME", "{{timestamp}}"))
                .isInstanceOf(ConfigException.class).withMessage(
                        "Invalid value {{timestamp}} for configuration CONFIGURATION_NAME template variable 'timestamp': parameter 'unit' must be specified and string must be one of: yyyy, MM, dd, HH");

    }
}

