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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parser for a template
 */
public final class TemplateParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateParser.class);

    /** Matches <code>{{ var:foo=bar }}</code> */
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{\\{\\s*([\\w]+)?(:?)([\\w=]+)?\\s*}}");

    /** Matches <code>foo=bar</code> */
    private static final Pattern PARAMETER_PATTERN = Pattern.compile("([\\w]+)?=?([\\w]+)?");

    private TemplateParser() {
    }

    /**
     * Parses the template.
     * @param template the template string.
     * @param registry the template variable registry.
     * @return The parsed template object.
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public static Template parse(final String template, final TemplateVariableRegistry registry) {
        LOGGER.debug("Parse template: {}", template);

        final List<Pair<String, Parameter>> variablesAndParametersBuilder = new ArrayList<>();
        final List<TemplatePart> templatePartsBuilder = new ArrayList<>();

        final Matcher matcher = VARIABLE_PATTERN.matcher(template);

        int position = 0;
        while (matcher.find()) {
            if (position < matcher.start()) {
                templatePartsBuilder.add(new TextTemplatePart(template.substring(position, matcher.start())));
            }

            final String variable = matcher.group(1);

            if (Objects.isNull(variable)) {
                throw new ConfigException(String.format("Variable name hasn't been set for template: %s", template));
            }
            final String errName = String.format("template variable '%s'", variable);

            Optional<TemplateVariable> templateVariable = registry != null && registry.has(variable) ? Optional.of(registry.get(variable)) : Optional.empty();
            if (registry != null && templateVariable.isEmpty()) {
                throw new ConfigException(String.format("Configuration template variable '%s' is not defined in the variable registry: %s", variable, template));
            }

            final String parameterDef = matcher.group(2);
            final String parameter = matcher.group(3);
            if (":".equals(parameterDef) && Objects.isNull(parameter)) { // NOPMD AvoidLiteralsInIfCondition
                throw new ConfigException(errName, "incomplete parameter definition", template);
            }

            final Parameter parseParameter = parseParameter(variable, template, parameter);
            templateVariable.ifPresent( tv -> tv.validate(parseParameter));

            variablesAndParametersBuilder.add(Pair.of(variable, parseParameter));
            templatePartsBuilder.add(new VariableTemplatePart(variable, parseParameter, matcher.group()));
            position = matcher.end();
        }
        if (template.length() > position) {
            templatePartsBuilder.add(new TextTemplatePart(template.substring(position)));
        }

        return new Template(template, Collections.unmodifiableList(variablesAndParametersBuilder), Collections.unmodifiableList(templatePartsBuilder));
    }

    private static Parameter parseParameter(final String variable, final String template, final String parameter) {
        LOGGER.debug("Parse {} parameter", parameter);
        if (Objects.nonNull(parameter)) {
            String errName= String.format("template variable '%s'", variable);
            final Matcher matcher = PARAMETER_PATTERN.matcher(parameter);
            if (!matcher.find()) {
                throw new ConfigException(errName, "parameter has not been set", template);
            }

            final String name = matcher.group(1);
            if (Objects.isNull(name)) {
                throw new ConfigException(errName, "parameter name has not been set", template);
            }

            final String value = matcher.group(2);
            if (Objects.isNull(value)) {
                throw new ConfigException(errName, String.format("parameter `%s` value has not been set", name), template);
            }

            return Parameter.of(name, value);
        } else {
            return Parameter.EMPTY;
        }
    }

}
