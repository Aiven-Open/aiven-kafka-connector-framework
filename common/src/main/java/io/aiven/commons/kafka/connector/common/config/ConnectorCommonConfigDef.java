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

import static io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment.DATA_COMPRESSION_TYPE;

import io.aiven.commons.kafka.config.CommonConfigDef;
import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import java.util.Map;
import org.apache.kafka.common.config.ConfigValue;

/** The configuration definition for the common connector. */
public class ConnectorCommonConfigDef extends CommonConfigDef {

  /** Constructor. */
  public ConnectorCommonConfigDef() {
    super();
    ConnectorCommonConfigFragment.update(this);
  }

  /**
   * This method hides the compression type from documentation but does not make it unconfigurable
   */
  protected void hideCompressionType() {
    ExtendedConfigKey newKey =
        ExtendedConfigKey.Builder.unbuild(configKeys().get(DATA_COMPRESSION_TYPE))
            .internalConfig(true)
            .build();
    configKeys().put(newKey.name, newKey);
  }

  @Override
  public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
    final Map<String, ConfigValue> values = super.multiValidate(valueMap);
    final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
    new ConnectorCommonConfigFragment(fragmentDataAccess).validate(values);
    return values;
  }
}
