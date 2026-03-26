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
package io.aiven.commons.kafka.connector.source.extractor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ExtractorRegistryTest {
  private static int FEATURE1 = 1;
  private static int FEATURE2 = 1 << 1;
  private static int PRIVATE_FEATURE1 = 1 << (ExtractorInfo.PRIVATE_FEATURE_SHIFT + 0);

  private ExtractorInfo[] infos = {
    new ExtractorInfo("no features", Extractor.class, ExtractorInfo.FEATURE_NONE, "mo features"),
    new ExtractorInfo("feat1", Extractor.class, FEATURE1, "the first feature"),
    new ExtractorInfo("feat2", Extractor.class, FEATURE2, "the second feature"),
    new ExtractorInfo("feat3", Extractor.class, FEATURE1 | FEATURE2, "oooo two features"),
    new ExtractorInfo("feat4", Extractor.class, 4, "A feature not defined with a constant"),
    new ExtractorInfo("private", Extractor.class, PRIVATE_FEATURE1, "The private feature"),
    new ExtractorInfo(
        "private + 1",
        Extractor.class,
        PRIVATE_FEATURE1 | FEATURE1,
        "private feature and then some"),
    new ExtractorInfo(
        "private + 2",
        Extractor.class,
        PRIVATE_FEATURE1 | FEATURE2,
        "private featuer and the some different."),
    new ExtractorInfo(
        "private + 3",
        Extractor.class,
        PRIVATE_FEATURE1 | FEATURE1 | FEATURE2,
        "private feature and then some more"),
    new ExtractorInfo(
        "private + 4",
        Extractor.class,
        PRIVATE_FEATURE1 | 4,
        "private feature and then our undefined  feature")
  };

  private ExtractorRegistry underTest = ExtractorRegistry.builder().add(infos).build();

  @Test
  void get() {
    assertThat(underTest.get("no features")).isNotNull();
    assertThat(underTest.get("missing")).isNull();
    assertThat(underTest.get("No features")).isNull();
    ExtractorRegistry registry2 =
        ExtractorRegistry.builder()
            .add(underTest)
            .add(
                new ExtractorInfo(
                    "No features",
                    Extractor.class,
                    ExtractorInfo.FEATURE_NONE,
                    "Name with a capital letter"))
            .build();
    assertThat(registry2.get("No features")).isNotNull();
  }

  @Test
  void getIgnoreCase() {
    assertThat(underTest.getIgnoreCase("no features")).isNotNull();
    assertThat(underTest.getIgnoreCase("missing")).isNull();
    assertThat(underTest.getIgnoreCase("No features")).isNotNull();

    ExtractorRegistry registry2 =
        ExtractorRegistry.builder()
            .add(underTest)
            .add(
                new ExtractorInfo(
                    "No features",
                    Extractor.class,
                    ExtractorInfo.FEATURE_NONE,
                    "Name with a capital letter"))
            .build();
    assertThat(registry2.getIgnoreCase("no features")).isNotNull();
    assertThat(registry2.getIgnoreCase("missing")).isNull();
    assertThat(registry2.getIgnoreCase("No features")).isNotNull();

    assertThat(registry2.getIgnoreCase("no features").commonName()).isEqualTo("No features");
    assertThat(registry2.getIgnoreCase("No features").commonName()).isEqualTo("No features");
    assertThat(registry2.getIgnoreCase("no Features").commonName()).isEqualTo("No features");
  }

  @Test
  void list() {
    assertThat(underTest.list()).containsExactlyInAnyOrder(infos);
  }

  @Test
  void any() {
    assertThat(underTest.any()).isNotNull();
    assertThat(ExtractorRegistry.builder().build().any()).isNull();
  }

  @Test
  void anyFeature() {
    assertThat(underTest.anyFeature(FEATURE1).stream().map(ExtractorInfo::commonName).toList())
        .containsExactlyInAnyOrder("feat1", "feat3", "private + 1", "private + 3");
    assertThat(underTest.anyFeature(FEATURE2).stream().map(ExtractorInfo::commonName).toList())
        .containsExactlyInAnyOrder("feat2", "feat3", "private + 2", "private + 3");
    assertThat(
            underTest.anyFeature(PRIVATE_FEATURE1).stream().map(ExtractorInfo::commonName).toList())
        .containsExactlyInAnyOrder(
            "private", "private + 1", "private + 2", "private + 3", "private + 4");
    assertThat(
            underTest.anyFeature(FEATURE1 | FEATURE2).stream()
                .map(ExtractorInfo::commonName)
                .toList())
        .containsExactlyInAnyOrder(
            "feat1", "feat2", "feat3", "private + 1", "private + 2", "private + 3");
  }

  @Test
  void allFeatures() {
    assertThat(underTest.allFeatures(FEATURE1).stream().map(ExtractorInfo::commonName).toList())
        .containsExactlyInAnyOrder("feat1", "feat3", "private + 1", "private + 3");
    assertThat(underTest.allFeatures(FEATURE2).stream().map(ExtractorInfo::commonName).toList())
        .containsExactlyInAnyOrder("feat2", "feat3", "private + 2", "private + 3");
    assertThat(
            underTest.allFeatures(PRIVATE_FEATURE1).stream()
                .map(ExtractorInfo::commonName)
                .toList())
        .containsExactlyInAnyOrder(
            "private", "private + 1", "private + 2", "private + 3", "private + 4");
    assertThat(
            underTest.allFeatures(FEATURE1 | FEATURE2).stream()
                .map(ExtractorInfo::commonName)
                .toList())
        .containsExactlyInAnyOrder("feat3", "private + 3");
  }
}
