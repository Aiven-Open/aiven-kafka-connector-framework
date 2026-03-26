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

import java.util.Map;
import java.util.function.Function;

/** A template part tht contains a variable definition. */
public class VariableTemplatePart implements TemplatePart {

  private final String variableName;

  private final Parameter parameter;

  private final String originalPlaceholder;

  VariableTemplatePart(final String variableName, final String originalPlaceholder) {
    this(variableName, Parameter.EMPTY, originalPlaceholder);
  }

  VariableTemplatePart(
      final String variableName, final Parameter parameter, final String originalPlaceholder) {
    this.variableName = variableName;
    this.parameter = parameter;
    this.originalPlaceholder = originalPlaceholder;
  }

  /**
   * Returns the variable name.
   *
   * @return the variable name.
   */
  public final String getVariableName() {
    return variableName;
  }

  /**
   * Determines if this variable template has a parameter associated.
   *
   * @return {@code true} if a parameter is available.
   */
  public final boolean hasParameter() {
    return !parameter.isEmpty();
  }

  /**
   * Returns the associated parameter.
   *
   * @return the associated parameter. Will return {@link Parameter#EMPTY} if there is no parameter.
   */
  public final Parameter getParameter() {
    return hasParameter() ? parameter : Parameter.EMPTY;
  }

  /**
   * Returns the original placeholder for the template. This is the variable pattern with the
   * enclosing curly braces.
   *
   * @return the original placeholder.
   */
  public final String getOriginalPlaceholder() {
    return originalPlaceholder;
  }

  @Override
  public String render(final Map<String, Function<Parameter, String>> bindings) {
    final Function<Parameter, String> binding = bindings.get(variableName);
    return binding == null ? originalPlaceholder : binding.apply(parameter);
  }

  @Override
  public String extract(final Map<String, String> captureGroups) {
    String capture = captureGroups.get(variableName);
    if (capture != null) {
      // If the name was already used, we can capture it with a backreference.
      return String.format("\\k<%s>", capture);
    }
    String name = "capture" + captureGroups.size();
    captureGroups.put(variableName, name);
    return String.format("(?<%s>.*?)", name);
  }
}
