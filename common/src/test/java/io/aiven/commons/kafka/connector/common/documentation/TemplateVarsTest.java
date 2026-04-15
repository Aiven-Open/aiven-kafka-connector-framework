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
package io.aiven.commons.kafka.connector.common.documentation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.junit.jupiter.api.Test;

/** Tests for the template variables */
public class TemplateVarsTest {

  TemplateVarsTest() {}

  @Test
  void visibilityInVelocityTest() {

    Velocity.setProperty("resource.loader", "testApp");
    Velocity.setProperty("testApp.resource.loader.class", ClasspathResourceLoader.class.getName());
    Velocity.init();

    VelocityContext context = new VelocityContext();

    TemplateVarsFactory factory = new TemplateVarsFactory();

    context.put("TemplateVarsFactory", factory);

    Template template = Velocity.getTemplate("TemplateVarsTest.vm");

    StringWriter sw = new StringWriter();

    template.merge(context, sw);
    String result = sw.toString();
    assertThat(result).contains("Var Name: key");
    assertThat(result).contains("Var Description: The key from the Kafka source record");
    assertThat(result).contains("Example: '{{ key }}'");
    assertThat(result).contains("Example: '{{ partition:padding=[true, false] }}'");
    assertThat(result).contains("        Parameter: padding");
    assertThat(result).contains("        Specifies that the value should be left padded");
    assertThat(result).contains("          - required: false");
    assertThat(result).contains("          - values: [true, false]");
  }
}
