/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.commons.kafka.connector.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for ConnectorCommonConfigFragment
 */
public class ConnectorCommonConfigFragmentTest {

  /**
	 * Constructor.
	 */
	public ConnectorCommonConfigFragmentTest() {}

	@ParameterizedTest(name = "{index} {0}")
	@MethodSource("testInvalidConfigData")
	void testInvalidConfig(String _ignore, Map<String, String> props, String[] expectedMessages) {
    assertThatThrownBy(() -> new ConnectorCommonConfig(new ConnectorCommonConfigDef(), props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContainingAll(expectedMessages);
  }

  static Stream<Arguments> testInvalidConfigData() {
    List<Arguments> lst = new ArrayList<>();
    String schemaErr =
        String.format(
            "Invalid value true for configuration %1$s: %1$s may not be 'true' if %2$s is unset.",
            ConnectorCommonConfigFragment.SCHEMA_REGISTRY_ENABLE,
            ConnectorCommonConfigFragment.SCHEMA_REGISTRY_URL);
    String keyErr =
        String.format(
            "Invalid value true for configuration %1$s: %1$s may not be 'true' if both %2$s and %3$s are unset.",
            ConnectorCommonConfigFragment.KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE,
            ConnectorCommonConfigFragment.KEY_CONVERTER_SCHEMA_REGISTRY_URL,
            ConnectorCommonConfigFragment.SCHEMA_REGISTRY_URL);

    String valueErr =
        String.format(
            "Invalid value true for configuration %1$s: %1$s may not be 'true' if both %2$s and %3$s are unset.",
            ConnectorCommonConfigFragment.VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE,
            ConnectorCommonConfigFragment.VALUE_CONVERTER_SCHEMA_REGISTRY_URL,
            ConnectorCommonConfigFragment.SCHEMA_REGISTRY_URL);

    Map<String, String> props = new HashMap<>();
    ConnectorCommonConfigFragment.Setter setter = ConnectorCommonConfigFragment.setter(props);
    setter.enableSchemaRegistry(true);
    lst.add(Arguments.of("no registry URL", props, new String[] {schemaErr}));

    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.enableKeyConverterRegistry(true);
    lst.add(Arguments.of("no key URL", props, new String[] {keyErr}));

    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.enableValueConverterRegistry(true);
    lst.add(Arguments.of("no value URL", props, new String[] {valueErr}));

    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.enableSchemaRegistry(true);
    setter.enableKeyConverterRegistry(true);
    setter.enableValueConverterRegistry(true);
    lst.add(Arguments.of("multiple errors", props, new String[] {schemaErr, keyErr, valueErr}));

    return lst.stream();
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testSchemaRegistryData")
  void testSchemaRegistry(
      final String _ignore,
      final Map<String, String> props,
      String schemaUrl,
      boolean schemaEnabled,
      String keyUrl,
      boolean keyEnabled,
      String valueUrl,
      boolean valueEnabled) {

    ConnectorCommonConfig commonConfig =
        new ConnectorCommonConfig(new ConnectorCommonConfigDef(), props);
    FragmentDataAccess dataAccess = FragmentDataAccess.from(commonConfig);
    ConnectorCommonConfigFragment fragment = new ConnectorCommonConfigFragment(dataAccess);

    assertThat(fragment.getSchemaRegistryUrl()).as("Schema URL").isEqualTo(schemaUrl);
    assertThat(fragment.isSchemaRegistryEnabled())
        .as("Schema enabled flag")
        .isEqualTo(schemaEnabled);
    assertThat(fragment.getKeyConverterSchemaRegistryUrl()).as("Key URL").isEqualTo(keyUrl);
    assertThat(fragment.isKeyConverterRegistryEnabled())
        .as("Key enabled flag")
        .isEqualTo(keyEnabled);
    assertThat(fragment.getValueConverterSchemaRegistryUrl()).as("Value URL").isEqualTo(valueUrl);
    assertThat(fragment.isValueConverterRegistryEnabled())
        .as("Value enabled flag")
        .isEqualTo(valueEnabled);
  }

  static Stream<Arguments> testSchemaRegistryData() {
    List<Arguments> lst = new ArrayList<>();
    final String schemaUrl = "https://example.com";
    final String keyUrl = "https://example.com/key";
    final String valueUrl = "https://example.com/value";

    lst.add(
        Arguments.of(
            "no params", new HashMap<String, String>(), null, false, null, false, null, false));

    /*
     * Schema URL provided,
     */
    // Schema only
    Map<String, String> props = new HashMap<>();
    ConnectorCommonConfigFragment.Setter setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    lst.add(Arguments.of("schema only", props, schemaUrl, true, schemaUrl, true, schemaUrl, true));

    // Schema only key off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableKeyConverterRegistry(false);
    lst.add(
        Arguments.of(
            "schema only, key off", props, schemaUrl, true, schemaUrl, false, schemaUrl, true));

    // Schema only value off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableValueConverterRegistry(false);
    lst.add(
        Arguments.of(
            "schema only, value off", props, schemaUrl, true, schemaUrl, true, schemaUrl, false));

    // Schema only key and value off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableKeyConverterRegistry(false);
    setter.enableValueConverterRegistry(false);
    lst.add(
        Arguments.of(
            "schema only, key and value off",
            props,
            schemaUrl,
            true,
            schemaUrl,
            false,
            schemaUrl,
            false));

    // Schema only, schema off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableSchemaRegistry(false);
    lst.add(
        Arguments.of(
            "schema only, schema off",
            props,
            schemaUrl,
            false,
            schemaUrl,
            false,
            schemaUrl,
            false));

    // Schema only, schema off, key on
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableSchemaRegistry(false);
    setter.enableKeyConverterRegistry(true);
    lst.add(
        Arguments.of(
            "schema only, schema off, key on",
            props,
            schemaUrl,
            false,
            schemaUrl,
            true,
            schemaUrl,
            false));

    // Schema only, schema off, value on
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableSchemaRegistry(false);
    setter.enableValueConverterRegistry(true);
    lst.add(
        Arguments.of(
            "schema only, schema off, value on",
            props,
            schemaUrl,
            false,
            schemaUrl,
            false,
            schemaUrl,
            true));

    // Schema only off, key and value on
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.enableSchemaRegistry(false);
    setter.enableKeyConverterRegistry(true);
    setter.enableValueConverterRegistry(true);
    lst.add(
        Arguments.of(
            "schema only, schema off, key and value on",
            props,
            schemaUrl,
            false,
            schemaUrl,
            true,
            schemaUrl,
            true));

    /*
     * Schema and Key URL provided,
     */

    // Schema and key
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.keyConverterSchemaRegistry(keyUrl);
    lst.add(Arguments.of("schema and key", props, schemaUrl, true, keyUrl, true, schemaUrl, true));

    // Schema and key, schema off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.keyConverterSchemaRegistry(keyUrl);
    setter.enableSchemaRegistry(false);
    lst.add(
        Arguments.of(
            "schema and key, schema off", props, schemaUrl, false, keyUrl, true, schemaUrl, false));

    // Schema and key, value off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.keyConverterSchemaRegistry(keyUrl);
    setter.enableValueConverterRegistry(false);
    lst.add(
        Arguments.of(
            "schema and key, value off", props, schemaUrl, true, keyUrl, true, schemaUrl, false));

    // Schema and key, schema and value off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.keyConverterSchemaRegistry(keyUrl);
    setter.enableSchemaRegistry(false);
    setter.enableValueConverterRegistry(false);
    lst.add(
        Arguments.of(
            "schema and key, key and value off",
            props,
            schemaUrl,
            false,
            keyUrl,
            true,
            schemaUrl,
            false));

    /*
     * Schema and Value URL provided,
     */
    // Schema and value,
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.valueConverterSchemaRegistry(valueUrl);
    lst.add(
        Arguments.of("schema and value", props, schemaUrl, true, schemaUrl, true, valueUrl, true));

    // Schema and value, key off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.valueConverterSchemaRegistry(valueUrl);
    setter.enableKeyConverterRegistry(false);
    lst.add(
        Arguments.of(
            "schema and value, key on", props, schemaUrl, true, schemaUrl, false, valueUrl, true));

    // Schema and value, schema off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.valueConverterSchemaRegistry(valueUrl);
    setter.enableSchemaRegistry(false);
    lst.add(
        Arguments.of(
            "schema and value, schema off",
            props,
            schemaUrl,
            false,
            schemaUrl,
            false,
            valueUrl,
            true));

    // Schema and value, schema and key off
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.schemaRegistry(schemaUrl);
    setter.valueConverterSchemaRegistry(valueUrl);
    setter.enableSchemaRegistry(false);
    setter.enableKeyConverterRegistry(true);
    setter.enableValueConverterRegistry(true);
    lst.add(
        Arguments.of(
            "schema and value, schema and key off",
            props,
            schemaUrl,
            false,
            schemaUrl,
            true,
            valueUrl,
            true));

    /*
     * Key URL only,
     */
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.keyConverterSchemaRegistry(keyUrl);
    lst.add(Arguments.of("key url only", props, null, false, keyUrl, true, null, false));

    /*
     * Value URL only,
     */
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.valueConverterSchemaRegistry(valueUrl);
    lst.add(Arguments.of("value url only", props, null, false, null, false, valueUrl, true));

    /*
     * Key and Value URL only,
     */
    props = new HashMap<>();
    setter = ConnectorCommonConfigFragment.setter(props);
    setter.keyConverterSchemaRegistry(keyUrl);
    setter.valueConverterSchemaRegistry(valueUrl);
    lst.add(
        Arguments.of("key and value url only", props, null, false, keyUrl, true, valueUrl, true));

    return lst.stream();
  }
}
