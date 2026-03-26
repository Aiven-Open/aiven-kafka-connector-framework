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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** A registry of template variables. */
public final class TemplateVariableRegistry {
  /** The registered template variables */
  private final Map<String, TemplateVariable> registeredVariables;

  /** A registry of the standard variables used in a Sink connector. */
  public static final TemplateVariableRegistry STANDARD_SINK =
      builder()
          .add(TemplateVariable.KEY)
          .add(TemplateVariable.TOPIC)
          .add(TemplateVariable.PARTITION)
          .add(TemplateVariable.START_OFFSET)
          .add(TemplateVariable.TIMESTAMP)
          .build();

  /**
   * Constructor.
   *
   * @param builder the builder to construct from.
   */
  private TemplateVariableRegistry(Builder builder) {
    registeredVariables = new TreeMap<>(builder.registeredVariables);
  }

  /**
   * Creates a new builder of TemplateVariableRegistries.
   *
   * @return the new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Determines if the name is in the registry.
   *
   * @param name the name to look for.
   * @return {@code true} if the name is in the registry.
   */
  public boolean has(String name) {
    return registeredVariables.containsKey(name);
  }

  /**
   * Returns a list of registered template variables.
   *
   * @return a list of registered template variables.
   */
  public List<TemplateVariable> listVariables() {
    return new ArrayList<>(registeredVariables.values());
  }

  /**
   * Gets the template variable for the specified name.
   *
   * @param name the name to retrieve.
   * @return the TemplateVariable.
   * @throws IllegalArgumentException if the variable is not in the registry.
   */
  public TemplateVariable get(final String name) {
    TemplateVariable result = registeredVariables.get(name);
    if (result == null) {
      throw new IllegalArgumentException(
          String.format("Unknown filename template variable: %s", name));
    }
    return result;
  }

  /** Builder for VariableTemplateRegistries. */
  public static class Builder {
    /** the map of variables for the new registry */
    private final Map<String, TemplateVariable> registeredVariables;

    /** Constructor. */
    private Builder() {
      registeredVariables = new HashMap<>();
    }

    /**
     * Builds a new TemplateVariableRegistry from this builder.
     *
     * @return a new TemplateVariableRegistry
     */
    public TemplateVariableRegistry build() {
      return new TemplateVariableRegistry(this);
    }

    /**
     * Adds a template variable to the builder.
     *
     * @param variable the variable to add.
     * @return this
     */
    public Builder add(TemplateVariable variable) {
      registeredVariables.put(variable.getName(), variable);
      return this;
    }

    /**
     * Removes a template variable from the builder.
     *
     * @param variable the variable to remove.
     * @return this
     */
    public Builder remove(TemplateVariable variable) {
      registeredVariables.remove(variable.getName());
      return this;
    }

    /**
     * Adds a template variable registry to the builder.
     *
     * @param registry the registry to add.
     * @return this
     */
    public Builder add(TemplateVariableRegistry registry) {
      registeredVariables.putAll(registry.registeredVariables);
      return this;
    }

    /**
     * Removes a template variable registry from the builder.
     *
     * @param registry the registry to remove.
     * @return this
     */
    public Builder remove(TemplateVariableRegistry registry) {
      registry.registeredVariables.keySet().forEach(registeredVariables::remove);
      return this;
    }
  }
}
