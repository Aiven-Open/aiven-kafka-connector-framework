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
package io.aiven.commons.kafka.connector.common.config;

import io.aiven.commons.kafka.config.CommonConfig;
import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.config.SinceInfo;
import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.ConfigFragment;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.config.validator.UrlValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.net.URI;
import java.util.Map;

/**
 * The common connector fragment.
 */
public class ConnectorCommonConfigFragment extends ConfigFragment {
	/** name of java property that if set allows http to be used in URLs */
	public static final String RELAX_SCHEMES = "io.aiven.commons.kafka.connector.common.config.RELAX_SCHEMES";
	/** the schema registry URL */
	static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
	/**
	 * The flag to enable the schema registry. The schema registry enablement flag.
	 * Defaults to {@code true} if {@link #SCHEMA_REGISTRY_URL} is set.
	 */
	static final String SCHEMA_REGISTRY_ENABLE = "schema.registry.enable";
	/**
	 * The value converter registry URL. Defaults to the
	 * {@link #SCHEMA_REGISTRY_URL}.
	 */
	static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
	/**
	 * The flag to enable the value converter schema registry. Defaults to
	 * {@link #SCHEMA_REGISTRY_ENABLE} if
	 * {@link #VALUE_CONVERTER_SCHEMA_REGISTRY_URL} is set.
	 */
	static final String VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE = "value.converter.schema.registry.enable";
	/**
	 * The key converter registry URL. Defaults to the {@link #SCHEMA_REGISTRY_URL}.
	 */
	static final String KEY_CONVERTER_SCHEMA_REGISTRY_URL = "key.converter.schema.registry.url";
	/**
	 * The flag to enable the key converter schema registry. Defaults to
	 * {@link #SCHEMA_REGISTRY_ENABLE} if {@link #KEY_CONVERTER_SCHEMA_REGISTRY_URL}
	 * is set.
	 */
	static final String KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE = "key.converter.schema.registry.enable";

	/**
	 * Creates a Setter for this fragment.
	 *
	 * @param data
	 *            the data map to modify.
	 * @return the Setter
	 */
	public static Setter setter(final Map<String, String> data) {
		return new Setter(data);
	}

	/**
	 * Construct the ConfigFragment.
	 *
	 * @param dataAccess
	 *            the FragmentDataAccess that this fragment is associated with.
	 */
	public ConnectorCommonConfigFragment(final FragmentDataAccess dataAccess) {
		super(dataAccess);
	}

	/**
	 * Update the configuration definition with the properties for this fragment.
	 *
	 * @param configDef
	 *            the configuration definition to update.
	 * @return the updated configuration definition.
	 */
	public static ConfigDef update(final ConfigDef configDef) {
		SinceInfo.Builder siBuilder = SinceInfo.builder().groupId("io.aiven.commons")
				.artifactId("aiven-kafka-connector-framework");
		final String COMMON_GROUP = "Connector Common";
		int connectorCommon = 0;

		UrlValidator.Builder builder = UrlValidator.builder().schemes("https");
		if (System.getProperties().get(RELAX_SCHEMES) != null) {
			builder.schemes("http");
		}
		UrlValidator schemaUrlValidator = builder.build();
		configDef
				.define(ExtendedConfigKey.builder(SCHEMA_REGISTRY_URL).validator(schemaUrlValidator).group(COMMON_GROUP)
						.orderInGroup(++connectorCommon).documentation("The default schema registry URL.")
						.since(siBuilder.version("1.0.0").build()).build())
				.define(ExtendedConfigKey.builder(SCHEMA_REGISTRY_ENABLE).type(ConfigDef.Type.BOOLEAN)
						.group(COMMON_GROUP).orderInGroup(++connectorCommon)
						.documentation(
								"The schema registry enablement flag. If 'true' the schema registry will be enabled, "
										+ "if 'false' the registry will not be enabled. "
										+ String.format(
												"If not set the registry will be enabled if the %s parameter is set.",
												SCHEMA_REGISTRY_URL))
						.since(siBuilder.version("1.0.0").build()).build())
				.define(ExtendedConfigKey.builder(VALUE_CONVERTER_SCHEMA_REGISTRY_URL).validator(schemaUrlValidator)
						.group(COMMON_GROUP).orderInGroup(++connectorCommon)
						.documentation(String.format(
								"Schema registry URL for value converters.  If not specified the '%s' value will be used.",
								SCHEMA_REGISTRY_URL))
						.since(siBuilder.version("1.0.0").build()).build())
				.define(ExtendedConfigKey.builder(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE).type(ConfigDef.Type.BOOLEAN)
						.group(COMMON_GROUP).orderInGroup(++connectorCommon)
						.documentation(
								"The value converter schema registry enablement flag. If 'true' the value converter schema registry will be enabled, "
										+ "if 'false' the registry will not be enabled. "
										+ String.format(
												"If not set the value converter schema registry will be enabled if the %s parameter enabled.",
												SCHEMA_REGISTRY_ENABLE))
						.since(siBuilder.version("1.0.0").build()).build())
				.define(ExtendedConfigKey.builder(KEY_CONVERTER_SCHEMA_REGISTRY_URL).validator(schemaUrlValidator)
						.group(COMMON_GROUP).orderInGroup(++connectorCommon)
						.documentation(String.format(
								"Schema registry URL for key converters.  If not specified the '%s' value will be used.",
								SCHEMA_REGISTRY_URL))
						.since(siBuilder.version("1.0.0").build()).build())
				.define(ExtendedConfigKey.builder(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE).type(ConfigDef.Type.BOOLEAN)
						.group(COMMON_GROUP).orderInGroup(++connectorCommon)
						.documentation(
								"The key converter schema registry enablement flag. If 'true' the key converter schema registry will be enabled, "
										+ "if 'false' the registry will not be enabled. "
										+ String.format(
												"If not set the key converter schema registry will be enabled if the %s parameter enabled.",
												SCHEMA_REGISTRY_ENABLE))
						.since(siBuilder.version("1.0.0").build()).build());

		return configDef;
	}

	/**
	 * Ensure that the various URLs and enablements are set correctly.
	 *
	 * @param configMap
	 *            the change tracking map to process.
	 */
	public static void postProcess(CommonConfig.ChangeTrackingMap configMap) {

		if (configMap.get(SCHEMA_REGISTRY_URL) != null) {
			if (configMap.get(SCHEMA_REGISTRY_ENABLE) == null) {
				configMap.override(SCHEMA_REGISTRY_ENABLE, true);
			}

			if (configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_URL) == null) {
				configMap.override(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, configMap.get(SCHEMA_REGISTRY_URL));
				if (configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE) == null) {
					configMap.override(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE, configMap.get(SCHEMA_REGISTRY_ENABLE));
				}
			} else {
				if (configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE) == null) {
					configMap.override(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE, true);
				}
			}

			if (configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_URL) == null) {
				configMap.override(KEY_CONVERTER_SCHEMA_REGISTRY_URL, configMap.get(SCHEMA_REGISTRY_URL));
				if (configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE) == null) {
					configMap.override(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE, configMap.get(SCHEMA_REGISTRY_ENABLE));
				}
			} else {
				if (configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE) == null) {
					configMap.override(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE, true);
				}
			}

		} else {
			if (configMap.get(SCHEMA_REGISTRY_ENABLE) == null) {
				configMap.override(SCHEMA_REGISTRY_ENABLE, false);
			}
			// set the value enable default if needed
			if (configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE) == null) {
				configMap.override(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE,
						configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_URL) != null);
			}

			if (configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE) == null) {
				configMap.override(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE,
						configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_URL) != null);
			}
		}
	}

	@Override
	public void validate(Map<String, ConfigValue> configMap) {

		// prefix or prefix template
		final ConfigValue schemaReg = configMap.get(SCHEMA_REGISTRY_URL);
		final ConfigValue valueReg = configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
		final ConfigValue keyReg = configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_URL);
		final ConfigValue schemaEnable = configMap.get(SCHEMA_REGISTRY_ENABLE);
		final ConfigValue valueEnable = configMap.get(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE);
		final ConfigValue keyEnable = configMap.get(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE);

		/* Schema Processing */
		if (schemaEnable.value() == Boolean.TRUE && schemaReg.value() == null) {
			registerIssue(configMap, SCHEMA_REGISTRY_ENABLE, schemaEnable.value(),
					String.format("%s may not be 'true' if %s is unset", SCHEMA_REGISTRY_ENABLE, SCHEMA_REGISTRY_URL));
		}

		/* Value converter schema processing */
		if (valueEnable.value() != null && valueEnable.value() == Boolean.TRUE && valueReg.value() == null) {
			registerIssue(configMap, VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE, valueEnable.value(),
					String.format("%s may not be 'true' if both %s and %s are unset",
							VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE, VALUE_CONVERTER_SCHEMA_REGISTRY_URL,
							SCHEMA_REGISTRY_URL));
		}

		/* Key converter schema processing */
		if (keyEnable.value() != null && keyEnable.value() == Boolean.TRUE && keyReg.value() == null) {
			registerIssue(configMap, KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE, keyEnable.value(),
					String.format("%s may not be 'true' if both %s and %s are unset",
							KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE, KEY_CONVERTER_SCHEMA_REGISTRY_URL,
							SCHEMA_REGISTRY_URL));
		}
		super.validate(configMap);
	}

	/**
	 * Gets the schema registry URL.
	 * 
	 * @return the schema registry URL. May be {@code null}
	 */
	public String getSchemaRegistryUrl() {
		return getString(SCHEMA_REGISTRY_URL);
	}

	/**
	 * Gets the schema registry enabled flag. If {@code true} the system should
	 * utilize the schema registry.
	 * 
	 * @return the schema registry enabled flag.
	 */
	public boolean isSchemaRegistryEnabled() {
		return getBoolean(SCHEMA_REGISTRY_ENABLE);
	}

	/**
	 * Gets the value converter schema registry URL. May be {@code null} if not set
	 * and {@link #getSchemaRegistryUrl} returns null.
	 * 
	 * @return the value converter schema registry URL.
	 */
	public String getValueConverterSchemaRegistryUrl() {
		return getString(VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
	}

	/**
	 * Gets the value converter schema registry enabled flag. If {@code true} the
	 * system should utilize the value converter schema registry.
	 * 
	 * @return the value converter schema registry enabled flag.
	 */
	public boolean isValueConverterRegistryEnabled() {
		return getBoolean(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE);
	}

	/**
	 * Gets the key converter schema registry URL. May be {@code null} if not set
	 * and {@link #getSchemaRegistryUrl} returns null.
	 * 
	 * @return the key converter schema registry URL.
	 */
	public String getKeyConverterSchemaRegistryUrl() {
		return getString(KEY_CONVERTER_SCHEMA_REGISTRY_URL);
	}

	/**
	 * Gets the key converter schema registry enabled flag. If {@code true} the
	 * system should utilize the key converter schema registry.
	 * 
	 * @return the key converter schema registry enabled flag.
	 */
	public boolean isKeyConverterRegistryEnabled() {
		return getBoolean(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE);
	}

	/**
	 * Gets the setter for this fragment.
	 */
	public static class Setter extends AbstractFragmentSetter<Setter> {

		/**
		 * Constructor.
		 * 
		 * @param data
		 *            the data map to modify.
		 */
		protected Setter(Map<String, String> data) {
			super(data);
		}

		/**
		 * Sets the schema registry URL.
		 * 
		 * @param schemaRegistryUrl
		 *            the schema registry URL
		 * @return this.
		 */
		public Setter schemaRegistry(String schemaRegistryUrl) {
			return setValue(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
		}

		/**
		 * Sets the schema registry URL.
		 * 
		 * @param schemaRegistryUri
		 *            the schema registry URL
		 * @return this.
		 */
		public Setter schemaRegistry(URI schemaRegistryUri) {
			return setValue(SCHEMA_REGISTRY_URL, schemaRegistryUri.toString());
		}

		/**
		 * Sets the schema registry flag.
		 * 
		 * @param enableSchemaRegistry
		 *            the state of the schema registry flag.
		 * @return this
		 */
		public Setter enableSchemaRegistry(boolean enableSchemaRegistry) {
			return setValue(SCHEMA_REGISTRY_ENABLE, enableSchemaRegistry);
		}

		/**
		 * Sets the value converter schema registry URL.
		 * 
		 * @param registryUrl
		 *            the value converter schema registry URL
		 * @return this.
		 */
		public Setter valueConverterSchemaRegistry(String registryUrl) {
			return setValue(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, registryUrl);
		}

		/**
		 * Sets the value converter schema registry URL.
		 * 
		 * @param registryUri
		 *            the value converter schema registry URL
		 * @return this.
		 */
		public Setter valueConverterSchemaRegistry(URI registryUri) {
			return setValue(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, registryUri.toString());
		}

		/**
		 * Sets the value converter schema registry flag.
		 * 
		 * @param enableRegistry
		 *            the state of the value converter schema registry flag.
		 * @return this
		 */
		public Setter enableValueConverterRegistry(boolean enableRegistry) {
			return setValue(VALUE_CONVERTER_SCHEMA_REGISTRY_ENABLE, enableRegistry);
		}

		/**
		 * Sets the key converter schema registry URL.
		 * 
		 * @param registryUrl
		 *            the key converter schema registry URL
		 * @return this.
		 */
		public Setter keyConverterSchemaRegistry(String registryUrl) {
			return setValue(KEY_CONVERTER_SCHEMA_REGISTRY_URL, registryUrl);
		}

		/**
		 * Sets the key converter schema registry URL.
		 * 
		 * @param registryUri
		 *            the key converter schema registry URL
		 * @return this.
		 */
		public Setter keyConverterSchemaRegistry(URI registryUri) {
			return setValue(KEY_CONVERTER_SCHEMA_REGISTRY_URL, registryUri.toString());
		}

		/**
		 * Sets the value converter schema registry flag.
		 * 
		 * @param enableRegistry
		 *            the state of the key converter schema registry flag.
		 * @return this
		 */
		public Setter enableKeyConverterRegistry(boolean enableRegistry) {
			return setValue(KEY_CONVERTER_SCHEMA_REGISTRY_ENABLE, enableRegistry);
		}
	}
}
