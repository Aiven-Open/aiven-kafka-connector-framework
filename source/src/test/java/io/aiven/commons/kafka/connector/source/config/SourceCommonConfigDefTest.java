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
package io.aiven.commons.kafka.connector.source.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.kafka.config.docs.ConfigDefBean;
import io.aiven.commons.kafka.config.docs.ConfigDefBeanFactory;
import io.aiven.commons.kafka.config.docs.ExtendedConfigKeyBean;
import org.junit.jupiter.api.Test;

class SourceCommonConfigDefTest {
  ConfigDefBeanFactory factory = new ConfigDefBeanFactory();

  @Test
  void beanFactoryTest() {
    String[] nonExtended = {"errors.tolerance", "tasks.max"};
    ConfigDefBean<ExtendedConfigKeyBean> cdb =
        factory.open(SourceCommonConfig.SourceCommonConfigDef.class.getName());
    for (ExtendedConfigKeyBean eckb : cdb.configKeys()) {
      if (eckb.isExtendedFlag()) {
        assertThat(eckb.getName()).isNotIn(nonExtended).as(eckb.getName());
      } else {
        assertThat(eckb.getName()).isIn(nonExtended).as(eckb.getName());
      }
    }
  }
}
