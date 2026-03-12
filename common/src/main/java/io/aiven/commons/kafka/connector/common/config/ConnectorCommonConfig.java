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

import io.aiven.commons.io.compression.CompressionType;
import io.aiven.commons.kafka.config.CommonConfig;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;

import java.util.Map;

/**
 * The definition of the common connector config functions.
 */
public class ConnectorCommonConfig extends CommonConfig {

	private final ConnectorCommonConfigFragment connectorConfigFragment;
	/**
	 * Constructor.
	 * 
	 * @param configDef
	 *            the configuration definition.
	 * @param props
	 *            the map of properties
	 */
	public ConnectorCommonConfig(ConnectorCommonConfigDef configDef, Map<String, String> props) {
		super(configDef, props);
		final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
		connectorConfigFragment = new ConnectorCommonConfigFragment(dataAccess);
	}

	@Override
	protected void fragmentPostProcess(ChangeTrackingMap map) {
		super.fragmentPostProcess(map);
		ConnectorCommonConfigFragment.postProcess(map);
	}

	/**
	 * Gets the schema registry URL. May return {@code null} if not set.
	 * 
	 * @return the schema registry URL or {@code null}
	 */
	public String getSchemaRegistryUrl() {
		return connectorConfigFragment.getSchemaRegistryUrl();
	}

	/**
	 * Returns {@code true} if the schema registry is enabled. Schema registry will
	 * not be listed as enabled if the URL is null.
	 * 
	 * @return {@code true} if the registry is enabled, {@code false} otherwise.
	 */
	final public boolean isSchemaRegistryEnabled() {
		return connectorConfigFragment.isSchemaRegistryEnabled();
	}

	/**
	 * Gets the value converter schema registry URL. May return {@code null} if not
	 * set.
	 * 
	 * @return the value converter schema registry URL or {@code null}
	 */
	final public String getValueConverterSchemaRegistryUrl() {
		return connectorConfigFragment.getValueConverterSchemaRegistryUrl();
	}

	/**
	 * Returns {@code true} if the value converter schema registry is enabled.
	 * Schema registry will not be listed as enabled if the value converter schema
	 * registry URL is null.
	 * 
	 * @return {@code true} if the value converter registry is enabled,
	 *         {@code false} otherwise.
	 */
	final public boolean isValueConverterRegistryEnabled() {
		return connectorConfigFragment.isValueConverterRegistryEnabled();
	}

	/**
	 * Gets the key converter schema registry URL. May return {@code null} if not
	 * set.
	 * 
	 * @return the key converter schema registry URL or {@code null}
	 */
	final public String getKeyConverterSchemaRegistryUrl() {
		return connectorConfigFragment.getKeyConverterSchemaRegistryUrl();
	}

	/**
	 * Returns {@code true} if the key converter schema registry is enabled. Schema
	 * registry will not be listed as enabled if the key converter schema registry
	 * URL is null.
	 * 
	 * @return {@code true} if the key converter registry is enabled, {@code false}
	 *         otherwise.
	 */
	final public boolean isKeyConverterRegistryEnabled() {
		return connectorConfigFragment.isKeyConverterRegistryEnabled();
	}

	final public CompressionType getCompressionType() {
		return connectorConfigFragment.getCompressionType();
	}
}