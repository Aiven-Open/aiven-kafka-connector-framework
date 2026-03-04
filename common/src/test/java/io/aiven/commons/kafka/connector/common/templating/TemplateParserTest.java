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

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TemplateParserTest {

    /**
     * Meta-tests to ensure that the test utility detects the errors it claims and fails with meaningful messages.
     */
    @Test
    void testHelper() {
        // A basic example of a successful test
        assertThat(TemplateParser.parse("{{foo}}/{{bar}}", null))
                .satisfies(TemplateTestUtil.withBindings("foo", "FOO", "bar", "BAR").rendersTo("FOO/BAR"))
                .satisfies(TemplateTestUtil.withInput("FOO/BAR").extracts("foo", "FOO", "bar", "BAR"));

        // Failure when the test helper is misconfigured.
        assertThatThrownBy(() -> assertThat(TemplateParser.parse("{{foo}}/{{bar}}", null))
                .satisfies(TemplateTestUtil.withBindings("foo", "FOO", "bar").rendersTo("FOO/BAR"))).isInstanceOf(AssertionError.class)
                .hasMessageContaining("Must set names and values in pairs")
                .hasMessageContaining("Expecting 3 to be even");

        // Failure when the instance does not rendered as expected
        assertThatThrownBy(() -> assertThat(TemplateParser.parse("{{foo}}/{{bar}}/{{baz}}", null))
                .satisfies(TemplateTestUtil.withBindings("foo", "FOO", "bar", "BAR").rendersTo("FOO/bar")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("expected: \"FOO/bar\"")
                .hasMessageContaining("but was: \"FOO/BAR/{{baz}}\"");

        // Failure when the extractor does not extract as expected
        assertThatThrownBy(() -> assertThat(TemplateParser.parse("{{foo}}/{{bar}}", null))
                .satisfies(TemplateTestUtil.withInput("FOO/BAR").extracts("foo", "foo", "bar", "bar")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("but the following map entries had different values:")
                .hasMessageContaining("[\"foo\"=\"FOO\" (expected: \"foo\")]");

        // Failure when an expected variable value is missing
        assertThatThrownBy(
                () -> assertThat(TemplateParser.parse("{{foo}}/{{bar}}", null)).satisfies(TemplateTestUtil.withInput("FOO/BAR").extracts("foo", "FOO")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("to contain only following keys:")
                .hasMessageContaining("[\"foo\"]");

        // Failure when too many variables are expected in the returned
        assertThatThrownBy(() -> assertThat(TemplateParser.parse("{{foo}}/{{bar}}", null))
                .satisfies(TemplateTestUtil.withInput("FOO/BAR").extracts("foo", "FOO", "bar", "BAR", "baz", "BAZ")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("but could not find the following map entries:")
                .hasMessageContaining("[\"baz\"=\"BAZ\"]");
    }

    @Test
    void emptyString() {
        assertThat(TemplateParser.parse("", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo(""))
                .satisfies(TemplateTestUtil.withNoBindings().rendersTo(""))
                .satisfies(TemplateTestUtil.withInput("").extractsEmpty());
    }

    @Test
    void noVariables() {
        assertThat(TemplateParser.parse("somestring", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("somestring"))
                .satisfies(TemplateTestUtil.withNoBindings().rendersTo("somestring"))
                .satisfies(TemplateTestUtil.withInput("somestring").extractsEmpty());
    }

    @Test
    void newLine() {
        assertThat(TemplateParser.parse("some\nstring", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("some\nstring"))
                .satisfies(TemplateTestUtil.withNoBindings().rendersTo("some\nstring"))
                .satisfies(TemplateTestUtil.withInput("some\nstring").extractsEmpty());
    }

    @Test
    void emptyVariableName() {
        final String templateStr = "foo{{ }}bar";
        assertThatThrownBy(() -> TemplateParser.parse(templateStr, null)).isInstanceOf(ConfigException.class)
                .hasMessage("Variable name hasn't been set for template: foo{{ }}bar");
    }

    @Test
    void variableFormatNoSpaces() {
        assertThat(TemplateParser.parse("{{foo}}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatLeftSpace() {
        assertThat(TemplateParser.parse("{{ foo}}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatRightSpace() {
        assertThat(TemplateParser.parse("{{foo }}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatBothSpaces() {
        assertThat(TemplateParser.parse("{{ foo }}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatBothSpacesWithVariable() {
        assertThat(TemplateParser.parse("{{ foo:tt=true }}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void parseVariableWithParameter() {
        final Template template = TemplateParser.parse("{{ foo:tt=true }}", null);
        final String render = template.instance().bindVariable("foo", parameter -> {
            assertThat(parameter.getName()).isEqualTo("tt");
            assertThat(parameter.getValue()).isEqualTo("true");
            assertThat(parameter.asBoolean()).isTrue();
            return "PARAMETER_TESTED";
        }).render();

        assertThat(render).isEqualTo("PARAMETER_TESTED");
        assertThat(template).satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void invalidVariableWithoutParameter() {
        assertThatThrownBy(() -> TemplateParser.parse("{{foo:}}", null)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value incomplete parameter definition for configuration template variable 'foo': {{foo:}}");
    }

    @Test
    void invalidVariableWithEmptyVariableNameAndWithParameter() {
        assertThatThrownBy(() -> TemplateParser.parse("{{:foo=bar}}", null)).isInstanceOf(ConfigException.class)
                .hasMessage("Variable name hasn't been set for template: {{:foo=bar}}");
    }

    @Test
    void invalidVariableWithEmptyParameterValue() {
        assertThatThrownBy(() -> TemplateParser.parse("{{foo:tt=}}", null)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value parameter `tt` value has not been set for configuration template variable 'foo': {{foo:tt=}}");
    }

    @Test
    void invalidVariableWithoutParameterName() {
        assertThatThrownBy(() -> TemplateParser.parse("{{foo:=bar}}", null)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value parameter name has not been set for configuration template variable 'foo': {{foo:=bar}}");
    }

    @Test
    void variableFormatMultipleSpaces() {
        assertThat(TemplateParser.parse("{{   foo  }}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatTabs() {
        assertThat(TemplateParser.parse("{{\tfoo\t}}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableUnderscoreAlone() {
        assertThat(TemplateParser.parse("{{ _ }}", null)).satisfies(TemplateTestUtil.withBindings("_", "FOO").rendersTo("FOO"))
                .satisfies(TemplateTestUtil.withInput("FOO").extracts("_", "FOO"));
    }

    @Test
    void variableUnderscoreWithOtherSymbols() {
        assertThat(TemplateParser.parse("{{ foo_bar }}", null)).satisfies(TemplateTestUtil.withBindings("foo_bar", "FOO_BAR").rendersTo("FOO_BAR"))
                .satisfies(TemplateTestUtil.withInput("FOO_BAR").extracts("foo_bar", "FOO_BAR"));
    }

    @Test
    void placeholderHasCurlyBracesInside() {
        final String templateStr = "{{ { }}";
        assertThat(TemplateParser.parse(templateStr, null)).satisfies(TemplateTestUtil.withBindings("{", "FOO").rendersTo(templateStr))
                .satisfies(TemplateTestUtil.withInput(templateStr).extractsEmpty());
    }

    @Test
    void unclosedPlaceholder() {
        final String templateStr = "bb {{ aaa ";
        assertThat(TemplateParser.parse(templateStr, null)).satisfies(TemplateTestUtil.withBindings("aaa", "FOO").rendersTo(templateStr))
                .satisfies(TemplateTestUtil.withInput(templateStr).extractsEmpty());
    }

    @Test
    void variableInBeginning() {
        assertThat(TemplateParser.parse("{{ foo }} END", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO END"))
                .satisfies(TemplateTestUtil.withInput("FOO END").extracts("foo", "FOO"));
    }

    @Test
    void variableInMiddle() {
        assertThat(TemplateParser.parse("BEGINNING {{ foo }} END", null))
                .satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("BEGINNING FOO END"))
                .satisfies(TemplateTestUtil.withInput("BEGINNING FOO END").extracts("foo", "FOO"));
    }

    @Test
    void variableInEnd() {
        assertThat(TemplateParser.parse("BEGINNING {{ foo }}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("BEGINNING FOO"))
                .satisfies(TemplateTestUtil.withInput("BEGINNING FOO").extracts("foo", "FOO"));
    }

    @Test
    void nonBoundVariable() {
        // Note that the round trip is not completely reversible for unbound variables.
        assertThat(TemplateParser.parse("BEGINNING {{ foo }}", null)).satisfies(TemplateTestUtil.withNoBindings().rendersTo("BEGINNING {{ foo }}"))
                .satisfies(TemplateTestUtil.withInput("BEGINNING {{ foo }}").extracts("foo", "{{ foo }}"));
    }

    @Test
    void multipleVariables() {
        assertThat(TemplateParser.parse("1{{foo}}2{{bar}}3{{baz}}4", null))
                .satisfies(TemplateTestUtil.withBindings("foo", "FOO", "bar", "BAR", "baz", "BAZ").rendersTo("1FOO2BAR3BAZ4"))
                .satisfies(TemplateTestUtil.withInput("1FOO2BAR3BAZ4").extracts("foo", "FOO", "bar", "BAR", "baz", "BAZ"));
    }

    @Test
    void sameVariableMultipleTimes() {
        final Template template = TemplateParser.parse("{{foo}}{{foo}}{{foo}}", null);
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foofoofoo");
        assertThat(template.extractor().extract("foofoofoo")).hasSize(1).containsEntry("foo", "foo");
    }

    @Test
    void bigListOfNaughtyStringsJustString() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            assertThat(TemplateParser.parse(line, null)).satisfies(TemplateTestUtil.withNoBindings().rendersTo(line))
                    .satisfies(TemplateTestUtil.withInput(line).extractsEmpty());
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInBeginning() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            assertThat(TemplateParser.parse("{{ foo }}" + line, null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo("FOO" + line))
                    .satisfies(TemplateTestUtil.withInput("FOO" + line).extracts("foo", "FOO"));
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInEnd() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            assertThat(TemplateParser.parse(line + "{{ foo }}", null)).satisfies(TemplateTestUtil.withBindings("foo", "FOO").rendersTo(line + "FOO"))
                    .satisfies(TemplateTestUtil.withInput(line + "FOO").extracts("foo", "FOO"));
        }
    }

    private Collection<String> getBigListOfNaughtyStrings() throws IOException {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("blns.txt");
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.lines().filter(s -> !s.isEmpty() && !s.startsWith("#")).collect(Collectors.toList());
        }
    }

    @Test
    void variables() {
        final Template template = TemplateParser.parse("1{{foo}}2{{bar}}3{{baz}}4", null);
        assertThat(template.variables()).containsExactly("foo", "bar", "baz");
    }

    @Test
    void variablesWithRegistry() {
        final Template template = TemplateParser.parse("{{key}}{{topic}}", TemplateVariableRegistry.STANDARD_SINK);
        final Template.Instance instance = template.instance();
        instance.bindVariable("key", () -> "key").bindVariable("topic", () -> "topic");
        assertThat(instance.render()).isEqualTo("keytopic");
        // extractor will not work here.
    }

    @Test
    void missingVariablesFromWithRegistry() {
        assertThatException().isThrownBy(() -> TemplateParser.parse("{{key}}{{topic}}{{missing}}", TemplateVariableRegistry.STANDARD_SINK))
                .isInstanceOf(ConfigException.class)
                .withMessage("Configuration template variable 'missing' is not defined in the variable registry: {{key}}{{topic}}{{missing}}");

    }

    @Test
    void requiredParameterMissingWithRegistry() {
        assertThatException().isThrownBy(() -> TemplateParser.parse("{{key}}{{topic}}{{timestamp}}", TemplateVariableRegistry.STANDARD_SINK))
                .isInstanceOf(ConfigException.class)
                .withMessage("Invalid value null for configuration variable 'timestamp' parameter 'unit': String must be one of: yyyy, MM, dd, HH");

    }


}
