/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.commons.kafka.connector.common.documentation;

import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import org.apache.kafka.common.utils.Utils;
import org.apache.velocity.tools.config.DefaultKey;
import org.apache.velocity.tools.config.ValidScope;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Creates instances of TemplateVariableRegistry for generated documentation.
 */
@DefaultKey("TemplateVarsFactory")
@ValidScope({"application"})
public class TemplateVarsFactory {
	/**
	 * Constructs the TemplateVariableRegistry for the named class that will create
	 * an instance of TemplateVariableRegistry.
	 * 
	 * @param className
	 *            the class that is a supplier of the TemplateVariableRegistry
	 *            instance.
	 * @return a TemplateVariableRegistry instance..
	 */
	public TemplateVariableRegistry registry(String className) {
		try {
			final Class<Supplier<TemplateVariableRegistry>> clazz = (Class<Supplier<TemplateVariableRegistry>>) Utils
					.loadClass(className, Supplier.class);
			final Supplier<TemplateVariableRegistry> supplier = clazz.getConstructor().newInstance();
			return supplier.get();
		} catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException
				| NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}
}
