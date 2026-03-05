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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A parsed template string.
 * <p>
 * A template string is a string that has zero or more {@link TemplateVariable}s
 * defined. For example "This is the key {{ key }}, and this is the partition {{
 * partition:padding=true }}". To construct a {@code Template} use the
 * {@link TemplateParser}
 * </p>
 */
public final class Template {
	private static final Logger LOGGER = LoggerFactory.getLogger(Template.class);
	/** the list of parsed variables */
	private final List<VariableTemplatePart> variables;
	/** The list of all template parts */
	private final List<TemplatePart> templateParts;
	/** The original template pattern */
	private final String templatePattern;

	/**
	 * Constructor.
	 * 
	 * @param templatePattern
	 *            The textual representation of the template.
	 * @param templateParts
	 *            the template pars as parsed from the templatePattern.
	 */
	Template(final String templatePattern, final List<TemplatePart> templateParts) {
		this.templatePattern = templatePattern;
		this.templateParts = templateParts;
		this.variables = templateParts.stream().filter(VariableTemplatePart.class::isInstance)
				.map(VariableTemplatePart.class::cast).toList();
	}

	/**
	 * Return the original template pattern.
	 * 
	 * @return the template pattern.
	 */
	public String originalTemplate() {
		return templatePattern;
	}

	/**
	 * Creates a new list of variable names.
	 *
	 * @return A new list of variable names.
	 */
	public List<String> variables() {
		return variables.stream().map(VariableTemplatePart::getVariableName).collect(Collectors.toList());
	}

	/**
	 * Creates a new set of variable names.
	 *
	 * @return A new set of variable names.
	 */
	public Set<String> variablesSet() {
		return variables.stream().map(VariableTemplatePart::getVariableName).collect(Collectors.toSet());
	}

	/**
	 * Gets a list of variable names to parameters. If duplicate variables appeared
	 * in the template there will be duplicates in the list.
	 * 
	 * @return the list of variable names to parameters.
	 */
	public List<VariableTemplatePart> variablesWithParameters() {
		return new ArrayList<>(variables);
	}

	/**
	 * Gets a list of variable names to parameters where the parameters are known to
	 * be non empty. If duplicate variables appeared in the template there will be
	 * duplicates in the list.
	 * 
	 * @return the list of variable names to parameters.
	 */
	public List<VariableTemplatePart> variablesWithNonEmptyParameters() {
		return variables.stream().filter(VariableTemplatePart::hasParameter).collect(Collectors.toList());
	}

	/**
	 * Creates a bound builder.
	 * 
	 * @return a new BoundBuilder based on this template.
	 */
	public BoundBuilder boundBuilder() {
		return new BoundBuilder(variablesSet(), templatePattern);
	}

	/**
	 * Creates an extractor to parse variable values back from the rendered string.
	 * 
	 * @return a new Extractor.
	 */
	public Extractor extractor() {
		return new Extractor();
	}

	/**
	 * Creates a template from a template string.
	 * 
	 * @param template
	 *            the template string.
	 * @return the parsed template.
	 * @deprecated Use
	 *             {@link TemplateParser#parse(String, TemplateVariableRegistry)}
	 */
	@Deprecated
	@SuppressWarnings("PMD.ShortMethodName")
	public static Template of(final String template) {
		return TemplateParser.parse(template, null);
	}

	@Override
	public String toString() {
		return templatePattern;
	}

	/**
	 * A Template with bindings for the variables.
	 */
	public final class Bound {
		private final Map<String, Function<Parameter, String>> bindings;

		private Bound(final BoundBuilder builder) {
			this.bindings = new HashMap<>(builder.bindings);
		}

		/**
		 * Renders the template parts.
		 * 
		 * @return the rendered string.
		 */
		public String render() {
			final StringBuilder stringBuilder = new StringBuilder();
			templateParts.stream().map(templatePart -> templatePart.render(bindings)).forEach(stringBuilder::append);
			return stringBuilder.toString();
		}
	}

	/**
	 * A builder for Template.Bound instances.
	 */
	public class BoundBuilder {
		private final Map<String, Function<Parameter, String>> bindings = new HashMap<>();
		private final Set<String> variableNames;
		private final String templatePattern;

		private BoundBuilder(final Set<String> variableNames, final String templatePattern) {
			this.variableNames = variableNames;
			this.templatePattern = templatePattern;
		}

		/**
		 * Adds all the bindings from the builder to this builder. Will overwrite any
		 * bindings with the same name.
		 * 
		 * @param builder
		 *            the builder to copy bindings from.
		 * @return this.
		 */
		public BoundBuilder add(final BoundBuilder builder) {
			bindings.putAll(builder.bindings);
			return this;
		}

		/**
		 * Adds all the bindings from the Template.Bound to this builder.
		 * 
		 * @param bound
		 *            the Template.Bound to copy from.
		 * @return this
		 */
		public BoundBuilder add(final Bound bound) {
			bindings.putAll(bound.bindings);
			return this;
		}

		/**
		 * Bind the variable to the supplier of string.
		 * 
		 * @param name
		 *            the name to bind.
		 * @param binding
		 *            the binding.
		 * @throws IllegalArgumentException
		 *             if the variable is not in the template.
		 * @return this
		 */
		public BoundBuilder bind(final String name, final Supplier<String> binding) {
			Function<Parameter, String> func = x -> binding.get();
			return bind(name, func);
		}

		/**
		 * Bind the variable to a function to convert a parameter to a string.
		 * 
		 * @param name
		 *            the name to bind.
		 * @param binding
		 *            the function to bind.
		 * @throws IllegalArgumentException
		 *             if the variable is not in the template.
		 * @return this
		 */
		public BoundBuilder bind(final String name, final Function<Parameter, String> binding) {
			Objects.requireNonNull(name, "name cannot be null");
			Objects.requireNonNull(binding, "binding cannot be null");
			if (variableNames.contains(name)) {
				bindings.put(name, binding);
				return this;
			} else {
				throw new IllegalArgumentException(
						String.format("Parameter '%s' does not exist in the template '%s'", name, templatePattern));
			}
		}

		/**
		 * Builds the bound template. Will log any missing bindings.
		 * 
		 * @return the bound template.
		 */
		public Bound build() {
			if (LOGGER.isInfoEnabled()) {
				List<String> missingBindings = variableNames.stream().filter(name -> !bindings.containsKey(name))
						.toList();
				if (!missingBindings.isEmpty()) {
					LOGGER.info("The following variables are unbound: " + String.join(", ", missingBindings));
				}
			}
			return new Bound(this);
		}
	}

	/**
	 * Given a template, the {@link Extractor} finds the matching variables from a
	 * string.
	 * <p>
	 * Where an {@link Bound} generates strings by filling in the variables in a
	 * template, the {@link Extractor} does the opposite:
	 *
	 * <pre>
	 * // Renders the string "Hello World!"
	 * var tmpl = Template.of("Hello {{name}}!");
	 * var greeting = tmpl.instance().bindVariable("name", () -&gt; "World").render();
	 *
	 * // The other way around, extracts a name from a string:
	 * tmpl.extractor().extract(greeting); // returns a map {name=World}
	 * tmpl.extractor().extract("Hello everyone!"); // returns a map {name=everyone}
	 * </pre>
	 *
	 * The intention is that applying the map back to the template will yield the
	 * original string, but the round-trip is not always exact.
	 * <ul>
	 * <li>Rendering a string uses a supplier and the extractor returns a fixed
	 * string.</li>
	 * <li>Variable parameters are currently ignored (to be determined).</li>
	 * </ul>
	 */
	public final class Extractor {
		private final Pattern regex;
		private final Map<String, String> captureGroups = new HashMap<>();

		private Extractor() {
			// Build a regex to match the template pattern. Every variable part is mapped to
			// a capturing group
			// and every text part is quoted to require an exact match. Variable part
			// parameters are ignored.
			final StringBuilder textAsRegex = new StringBuilder();
			templateParts.stream().map(templatePart -> templatePart.extract(captureGroups))
					.forEach(textAsRegex::append);
			regex = Pattern.compile(textAsRegex.toString());
		}

		/**
		 * Performs the variable extraction from the input string.
		 *
		 * @param input
		 *            A string to extract variables from.
		 * @return An immutable map of key-value pairs extracted from the input string,
		 *         corresponding to the template variable names.
		 * @throws IllegalArgumentException
		 *             If the input string does not match the template.
		 */
		public Map<String, String> extract(final String input) {
			final Matcher matcher = regex.matcher(input);
			if (!matcher.matches()) {
				throw new IllegalArgumentException("Input does not match the template");
			}
			final Map<String, String> result = new HashMap<>();
			for (final Map.Entry<String, String> entry : captureGroups.entrySet()) {
				result.put(entry.getKey(), matcher.group(entry.getValue()));
			}
			return Collections.unmodifiableMap(result);
		}
	}

}
