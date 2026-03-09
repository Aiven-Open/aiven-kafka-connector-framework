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

	/** Matches invalid name characters */
	static final Pattern INVALID_NAME = Pattern.compile("\\W");

	static final Pattern INVALID_PATTERN = Pattern.compile("[^\\w:=]");

	private TemplateParser() {
	}

	/**
	 * Validates that the template string is parsable.
	 * 
	 * @param configurationName
	 *            the name of the template string. Generally the name of the
	 *            configuration option that provided the template. pattern.
	 * @param templatePattern
	 *            the template string.
	 * @param registry
	 *            the registry of permitted TemplateVariables
	 */
	public static void validate(final String configurationName, final String templatePattern,
			final TemplateVariableRegistry registry) {
		// generating the context performs the validation.
		Context context = new Context(configurationName, templatePattern, registry);
		// check for potential malformed templates
		context.templateParts.stream().map(tp -> tp instanceof TextTemplatePart txt ? txt.render(null) : "")
				.filter(StringUtils::isNotEmpty).forEach(text -> {
					for (String logMessage : checkMalformed(configurationName, text, registry)) {
						LOGGER.warn(logMessage);
					}
				});
	}

	/**
	 * Checks if a text string may contain malformed templates. If a valid template
	 * is passed it will be returned as potentially malformed.
	 * 
	 * @param name
	 *            the name of the template configuration option.
	 * @param text
	 *            the text that may be an invalid template, should not be a valid
	 *            template.
	 * @param registry
	 *            The TemplateVariableRegistry to validate against. May be
	 *            {@code null}.
	 * @return A list of log messages indicating the malformed templates.
	 */
	public static List<String> checkMalformed(final String name, final String text,
			final TemplateVariableRegistry registry) {
		List<String> logMessages = new ArrayList<>();
		String processing;
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
						if ((registry == null || registry.has(potentialName))
								&& VARIABLE_PATTERN.matcher(potential).matches()) {
							logMessages.add(
									String.format("Template text '%s' in %s may be a malformed template variable '%s'",
											candidate, name, potential));
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
	 * 
	 * @param templatePattern
	 *            the template string.
	 * @param registry
	 *            the template variable registry.
	 * @return The parsed template object.
	 */
	public static Template parse(final String templatePattern, final TemplateVariableRegistry registry) {
		LOGGER.debug("Parse template: {}", templatePattern);
		Context context = new Context(null, templatePattern, registry);
		return new Template(templatePattern, context.templateParts);
	}

	/**
	 * The context for a parsing event.
	 */
	private static class Context {
		private final List<TemplatePart> templateParts = new ArrayList<>();
		private final String configurationName;
		private final String templatePattern;
		private final TemplateVariableRegistry registry;

		private Context(final String configurationName, final String templatePattern,
				final TemplateVariableRegistry registry) {
			this.configurationName = configurationName;
			this.templatePattern = templatePattern;
			this.registry = registry;

			final Matcher matcher = VARIABLE_PATTERN.matcher(templatePattern);

			int position = 0;
			while (matcher.find()) {
				// add text or marker part -- we need a marker to extract the values.
				templateParts.add(new TextTemplatePart(templatePattern.substring(position, matcher.start())));

				validateVariablePattern(matcher.group());
				final String variableName = extractVariableName(matcher);

				final String errName = configurationName == null
						? String.format("template variable '%s'", variableName)
						: String.format("%s template variable '%s'", configurationName, variableName);

				Optional<TemplateVariable> templateVariable = createTemplateVariable(errName, variableName);
				Parameter parameter = extractParameter(errName, matcher);

				templateVariable.ifPresent(tv -> tv.validate(errName, templatePattern, parameter));

				templateParts.add(new VariableTemplatePart(variableName, parameter, matcher.group()));
				position = matcher.end();
			}
			if (templatePattern.length() > position) {
				templateParts.add(new TextTemplatePart(templatePattern.substring(position)));
			}
		}

		private Parameter extractParameter(final String errName, final Matcher matcher) {
			final String parameterDef = matcher.group(2);
			final String parameter = matcher.group(3);

			if (":".equals(parameterDef) && Objects.isNull(parameter)) { // NOPMD AvoidLiteralsInIfCondition
				throw new ConfigException(errName, "incomplete parameter definition", templatePattern);
			}

			return parseParameter(errName, parameter);
		}
		private Optional<TemplateVariable> createTemplateVariable(final String errName, final String variableName) {
			Optional<TemplateVariable> templateVariable = registry != null && registry.has(variableName)
					? Optional.of(registry.get(variableName))
					: Optional.empty();
			if (registry != null && templateVariable.isEmpty()) {
				throw new ConfigException(errName, templatePattern,
						String.format("'%s' is not defined in the variable registry", variableName));
			}
			return templateVariable;
		}

		private String extractVariableName(Matcher matcher) {
			final String variableName = matcher.group(1);
			if (Objects.isNull(variableName)) {
				if (configurationName == null) {
					throw new ConfigException(
							String.format("Variable name hasn't been set for template: %s", templatePattern));
				} else {
					throw new ConfigException(configurationName, templatePattern,
							"Variable name hasn't been set for template");
				}
			}
			return variableName;
		}

		private void validateVariablePattern(String variable) {
			String pattern = variable.substring(2, variable.length() - 2).trim();
			if (INVALID_PATTERN.matcher(pattern).find()) {
				String errName = configurationName == null
						? String.format("variable pattern '%s'", variable)
						: String.format("%s variable pattern '%s'", configurationName, variable);
				throw new ConfigException(errName, templatePattern, "variable pattern may not contain spaces");
			}
		}

		private Parameter parseParameter(final String errName, final String parameter) {
			LOGGER.debug("Parse {} parameter", parameter);
			if (Objects.nonNull(parameter)) {
				final Matcher matcher = PARAMETER_PATTERN.matcher(parameter);
				if (!matcher.find()) {
					throw new ConfigException(errName, "parameter has not been set", templatePattern);
				}

				final String parameterName = matcher.group(1);
				if (Objects.isNull(parameterName)) {
					throw new ConfigException(errName, "parameter name has not been set", templatePattern);
				}

				final String parameterValue = matcher.group(2);
				if (Objects.isNull(parameterValue)) {
					throw new ConfigException(errName,
							String.format("parameter `%s` value has not been set", parameterName), templatePattern);
				}

				return Parameter.of(parameterName, parameterValue);
			} else {
				return Parameter.EMPTY;
			}
		}
	}
}
