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

import java.util.Objects;

/** A parameter is a name/value pair */
public final class Parameter {

  /** An EMPTY parameter */
  public static final Parameter EMPTY = new Parameter(null, null);

  /** The parameter name */
  private final String name;

  /** The parameter value */
  private final String value;

  /**
   * Constructor.
   *
   * @param name the name of the property.
   * @param value the value.
   */
  private Parameter(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Determines if this is empty.
   *
   * @return {@code true} if this is empty.
   */
  public boolean isEmpty() {
    return name == null;
  }

  /**
   * Gets the parameter name.
   *
   * @return the parameter name.
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the parameter value.
   *
   * @return the parameter value.
   */
  public String getValue() {
    return value;
  }

  /**
   * Returns the parameter value as a boolean. Shorthand for {@code
   * Boolean.parseBoolean(getValue())}.
   *
   * @return the paramater value as a poolean.
   */
  public Boolean asBoolean() {
    return Boolean.parseBoolean(value);
  }

  /**
   * Creates a Parameter from a name and value.
   *
   * @param name the name (may not be null)
   * @param value the value (may not be null)
   * @return the parameter.
   */
  @SuppressWarnings("PMD.ShortMethodName")
  public static Parameter of(final String name, final String value) {
    if (Objects.isNull(name) && Objects.isNull(value)) {
      return EMPTY;
    } else {
      Objects.requireNonNull(name, "name has not been set");
      Objects.requireNonNull(value, "value has not been set");
      return new Parameter(name, value);
    }
  }

  @Override
  public String toString() {
    return name == null ? "EMPTY parameter" : name + "=" + value;
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    final Parameter parameter = (Parameter) other;
    return Objects.equals(name, parameter.name) && Objects.equals(value, parameter.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }
}
