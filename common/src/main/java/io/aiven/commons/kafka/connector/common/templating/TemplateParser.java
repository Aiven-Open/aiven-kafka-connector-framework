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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
     * Validates that the template string is parsable.
     * @param name the name of the template string.  Generally a configuration option name.
     * @param template the template string.
     * @param registry the registry.
     */
    public static void validate(final String name, final String template, final TemplateVariableRegistry registry) {
        // generating the context performs the validation.
        Context context = new Context(name, template, registry);
        // check for potential malformed templates
        context.templateParts.stream().map(tp ->
            tp instanceof TextTemplatePart txt ?
                txt.render(null) : ""
            ).filter(StringUtils::isNotEmpty)
                .forEach( text -> {
                    for (String logMessage : checkMalformed(name, text, registry)) {
                        LOGGER.warn(logMessage);
                    }
                });
    }

    /**
     * Checks if a text string may contain malformed templates.
     * If a valid template is passed it will be returned as potentially malformed.
     * @param name the name of the template configuration option.
     * @param text the text that may be an invalid template, should not be a valid template.
     * @return A list of log messages indicating the malformed templates.
     */
    public static List<String> checkMalformed(final String name, final String text, final TemplateVariableRegistry registry) {
        List<String> logMessages = new ArrayList<>();
        String processing = text;
        int start = 0;
        while (start > -1) {
            processing = text.substring(start);
            if (processing.contains("{{")) {
                int pos = processing.indexOf("{{");
                int end = processing.substring(pos).indexOf("}}");
                if (end > -1) {
                    // add the closing }}
                    end += pos + 2;
                    String candidate = processing.substring(pos, end);
                    if (candidate.contains(":") && candidate.contains("=")) {
                        String potential = String.join("", candidate.split("\\s+"));
                        String potentialName = potential.substring(2, potential.indexOf(":"));
                        if (registry == null || registry.has(potentialName)) {
                            if (VARIABLE_PATTERN.matcher(potential).matches()) {
                                logMessages.add(String.format("Template text '%s' in %s may be a malformed template variable '%s'", candidate, name, potential));
                            }
                        }
                    }
                }
                start += end;
            } else {
                start = -1;
            }
        }
        return logMessages;
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
        Context context = new Context(null, template, registry);

        return new Template(template, context.variablesAndParameters, context.templateParts);
    }

    private static class Context {
        private final List<Pair<String, Parameter>> variablesAndParameters = new ArrayList<>();
        private final List<TemplatePart> templateParts = new ArrayList<>();

        private Context(final String name, final String template, final TemplateVariableRegistry registry) {
            final Matcher matcher = VARIABLE_PATTERN.matcher(template);

            int position = 0;
            while (matcher.find()) {
                // add text or marker part -- we need a marker to extract the values.
                templateParts.add(new TextTemplatePart(template.substring(position, matcher.start())));

                final String variable = matcher.group(1);

                if (Objects.isNull(variable)) {
                    if (name == null) {
                        throw new ConfigException(String.format("Variable name hasn't been set for template: %s", template));
                    } else {
                        throw new ConfigException(name, template, "Variable name hasn't been set for template");
                    }

                }
                final String errName = name == null ? String.format("template variable '%s'", variable) : String.format("%s template variable '%s'", name, variable);

                Optional<TemplateVariable> templateVariable = registry != null && registry.has(variable) ? Optional.of(registry.get(variable)) : Optional.empty();
                if (registry != null && templateVariable.isEmpty()) {
                    throw new ConfigException(errName, template, String.format("'%s' is not defined in the variable registry", variable));
                }

                final String parameterDef = matcher.group(2);
                final String parameter = matcher.group(3);
                if (":".equals(parameterDef) && Objects.isNull(parameter)) { // NOPMD AvoidLiteralsInIfCondition
                    throw new ConfigException(errName, "incomplete parameter definition", template);
                }

                final Parameter parseParameter = parseParameter(errName, template, parameter);
                templateVariable.ifPresent(tv -> tv.validate(errName, template, parseParameter));

                variablesAndParameters.add(Pair.of(variable, parseParameter));
                templateParts.add(new VariableTemplatePart(variable, parseParameter, matcher.group()));
                position = matcher.end();
            }
            if (template.length() > position) {
                templateParts.add(new TextTemplatePart(template.substring(position)));
            }
        }

        private static Parameter parseParameter(final String errName, final String template, final String parameter) {
            LOGGER.debug("Parse {} parameter", parameter);
            if (Objects.nonNull(parameter)) {
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
}
