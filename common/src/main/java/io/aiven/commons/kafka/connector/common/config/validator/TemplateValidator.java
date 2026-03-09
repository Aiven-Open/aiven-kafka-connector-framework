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

import io.aiven.commons.kafka.connector.common.templating.TemplateParser;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.List;

/**
 * A validator for templates.
 */
public final class TemplateValidator implements ConfigDef.Validator {
	private final TemplateVariableRegistry registry;

	/**
	 * Creates a TemplateValidator that will, at a minimum, validate that the format
	 * of the template is correct. If the {@code registry} is provided additional
	 * checking for permitted variable names, parameter names and validation of
	 * parameter values will be attempted. If {@code registry} is {@code null} then
	 * all variable names will be accepted and no validation of parameters will be
	 * performed.
	 * 
	 * @param registry
	 *            the registry validation information.
	 */
	public TemplateValidator(TemplateVariableRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void ensureValid(String name, Object value) {
		TemplateParser.validate(name, value.toString(), registry);
	}

	@Override
	public String toString() {
		return registry == null ? "no restrictions" : generateList();
	}

	private String generateList() {
		List<String> vars = new ArrayList<>();
		registry.listVariables().forEach(tv -> vars.add(tv.getName()));
		return "[" + String.join(", ", vars) + "]";
	}
}
