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

import io.aiven.commons.kafka.config.CommonConfigDef;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Map;

/**
 * The configuration definition for the common connector.
 */
public class ConnectorCommonConfigDef extends CommonConfigDef {

	/**
	 * Constructor.
	 */
	public ConnectorCommonConfigDef() {
		super();
		ConnectorCommonConfigFragment.update(this);
	}

	@Override
	public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
		final Map<String, ConfigValue> values = super.multiValidate(valueMap);
		final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
		new ConnectorCommonConfigFragment(fragmentDataAccess).validate(values);
		return values;
	}

}
