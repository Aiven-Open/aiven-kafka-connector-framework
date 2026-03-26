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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import org.assertj.core.api.Condition;

/**
 * A helper class that can be used to test the rendering and extraction of a template in a fluent
 * way:
 *
 * <pre>
 * // To test that a template renders correctly with the given variables.
 * assertThat(Template.of("{{foo}}/{{bar}}")).satisfies(rendersTo("FOO/BAR").whenBinding("foo", "FOO", "bar", "BAR"));
 * </pre>
 *
 * <pre>
 * // To test that a template extracts the expected variables from the given
 * // string.
 * assertThat(Template.of("{{foo}}/{{bar}}")).satisfies(extracts("foo", "FOO", "bar", "BAR").whenGiven("FOO/BAR"));
 * </pre>
 */
class TemplateTestUtil {

  private final String rendered;
  private final String[] varNameAndValues;

  TemplateTestUtil(final String rendered, final String... varNameAndValues) {
    assertThat(varNameAndValues.length).as("Must set names and values in pairs").isEven();
    this.rendered = rendered;
    this.varNameAndValues = varNameAndValues; // NOPMD ArrayIsStoredDirectly
  }

  public static TemplateTestUtil withBindings(final String... varNameAndValues) {
    return new TemplateTestUtil("", varNameAndValues);
  }

  public static TemplateTestUtil withNoBindings() {
    return new TemplateTestUtil("");
  }

  public static TemplateTestUtil withInput(final String rendered) {
    return new TemplateTestUtil(rendered);
  }

  Condition<Template> rendersTo(final String rendered) {
    return new TemplateTestUtil(rendered, this.varNameAndValues).testRender();
  }

  Condition<Template> extracts(final String... varNameAndValues) {
    return new TemplateTestUtil(this.rendered, varNameAndValues).testExtract();
  }

  Condition<Template> extractsEmpty() {
    return new TemplateTestUtil(this.rendered).testExtract();
  }

  Condition<Template> variableNotSet(String var) {
    return new Condition<>(
        template -> {
          assertThat(template.variablesSet()).doesNotContain(var);
          return true;
        },
        String.format("Variable '%s' not set", var));
  }

  Condition<Template> noVariablesSet() {
    return new Condition<>(
        template -> {
          assertThat(template.variablesSet()).isEmpty();
          return true;
        },
        "No variables are set");
  }

  private Condition<Template> testRender() {
    return new Condition<>(
        template -> {
          final var boundBuilder = template.boundBuilder();
          for (int i = 0; i < varNameAndValues.length; i += 2) {
            final var value = varNameAndValues[i + 1];
            boundBuilder.bind(varNameAndValues[i], () -> value);
          }
          assertThat(boundBuilder.build().render()).isEqualTo(rendered);
          // Failed tests are indicated by assertions, not by this return value.
          return true;
        },
        "Renders to " + rendered);
  }

  private Condition<Template> testExtract() {
    return new Condition<>(
        template -> {
          final var extractor = template.extractor();
          final var extracted = extractor.extract(rendered);
          final var found = new HashSet<String>();
          for (int i = 0; i < varNameAndValues.length; i += 2) {
            assertThat(extracted).containsEntry(varNameAndValues[i], varNameAndValues[i + 1]);
            found.add(varNameAndValues[i]);
          }
          assertThat(extracted).containsOnlyKeys(found);
          // Failed tests are indicated by assertions, not by this return value.
          return true;
        },
        "Extracts " + rendered);
  }
}
