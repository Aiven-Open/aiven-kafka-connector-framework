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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ExtractorInfoTest {
  private static int FEATURE1 = 1;
  private static int FEATURE2 = 1 << 1;
  private static int PRIVATE_FEATURE1 = 1 << (ExtractorInfo.PRIVATE_FEATURE_SHIFT + 0);

  private ExtractorInfo underTest;

  @ParameterizedTest
  @CsvSource({
    "no features, 0, false, false, false",
    "feat1, 1, true, false, false",
    "feat2, 2, false, true, false",
    "feat3, 3, true, true, false",
    "feat4, 4, false, false, false",
    "private, 16777216, false, false, true",
    "private + 1, 16777217, true, false, true",
    "private + 2, 16777218, false, true, true",
    "private + 3, 16777219, true, true, true",
    "private + 4 , 16777220, false, false, true",
  })
  public void anyFeatures(String name, int flag, boolean f1, boolean f2, boolean f3) {
    underTest = new ExtractorInfo(name, Extractor.class, flag, "anyFeatures test");
    assertThat(underTest.anyFeatures(FEATURE1)).as("f1").isEqualTo(f1);
    assertThat(underTest.anyFeatures(FEATURE2)).as("f2").isEqualTo(f2);
    assertThat(underTest.anyFeatures(PRIVATE_FEATURE1)).as("private").isEqualTo(f3);
    assertThat(underTest.anyFeatures(FEATURE1 | FEATURE2)).as("f1 f2").isEqualTo(f1 || f2);
    assertThat(underTest.anyFeatures(FEATURE1 | PRIVATE_FEATURE1))
        .as("f1 private")
        .isEqualTo(f1 || f3);
    assertThat(underTest.anyFeatures(FEATURE2 | PRIVATE_FEATURE1))
        .as("f2 private")
        .isEqualTo(f2 || f3);
    assertThat(underTest.anyFeatures(FEATURE1 | FEATURE2 | PRIVATE_FEATURE1))
        .as("f1 f2 private")
        .isEqualTo(f1 || f2 || f3);
  }

  @ParameterizedTest
  @CsvSource({
    "no features, 0, false, false, false",
    "feat1, 1, true, false, false",
    "feat2, 2, false, true, false",
    "feat3, 3, true, true, false",
    "feat4, 4, false, false, false",
    "private, 16777216, false, false, true",
    "private + 1, 16777217, true, false, true",
    "private + 2, 16777218, false, true, true",
    "private + 3, 16777219, true, true, true",
    "private + 4 , 16777220, false, false, true",
  })
  public void allFeatures(String name, int flag, boolean f1, boolean f2, boolean f3) {
    underTest = new ExtractorInfo(name, Extractor.class, flag, "allFeatures test");
    assertThat(underTest.allFeatures(FEATURE1)).as("f1").isEqualTo(f1);
    assertThat(underTest.allFeatures(FEATURE2)).as("f2").isEqualTo(f2);
    assertThat(underTest.allFeatures(PRIVATE_FEATURE1)).as("private").isEqualTo(f3);
    assertThat(underTest.allFeatures(FEATURE1 | FEATURE2)).as("f1 f2").isEqualTo(f1 && f2);
    assertThat(underTest.allFeatures(FEATURE1 | PRIVATE_FEATURE1))
        .as("f1 private")
        .isEqualTo(f1 && f3);
    assertThat(underTest.allFeatures(FEATURE2 | PRIVATE_FEATURE1))
        .as("f2 private")
        .isEqualTo(f2 && f3);
    assertThat(underTest.allFeatures(FEATURE1 | FEATURE2 | PRIVATE_FEATURE1))
        .as("f1 f2 private")
        .isEqualTo(f1 && f2 && f3);
  }

  @ParameterizedTest
  @CsvSource({
    "no features, 0",
    "feat1, 1",
    "feat2, 2",
    "feat3, 3",
    "feat4, 4",
    "private, 16777216",
    "private, 16777217",
    "private, 16777218",
    "private, 16777219",
    "private, 16777220",
  })
  public void noFeatures(String name, int flag) {
    underTest = new ExtractorInfo(name, Extractor.class, flag, "noFeatures test");
    assertThat(underTest.noFeatures()).isEqualTo(flag == 0);
  }
}
