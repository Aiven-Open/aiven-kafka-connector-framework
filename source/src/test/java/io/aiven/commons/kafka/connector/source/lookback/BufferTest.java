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
package io.aiven.commons.kafka.connector.source.lookback;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferTest extends AbstractLookbackTest {
    private Buffer<String> underTest;

    @BeforeEach
    void setup() {
        underTest = new Buffer<>(3);
    }

    @Override
    @Test
    void addTest() {
        assertThat(underTest.contains("aKey")).isFalse();
        underTest.add("aKey");
        assertThat(underTest.contains("aKey")).isTrue();
        underTest.add("bKey");
        assertThat(underTest.contains("aKey")).isTrue();
        assertThat(underTest.contains("bKey")).isTrue();
        underTest.add("cKey");
        assertThat(underTest.contains("aKey")).isTrue();
        assertThat(underTest.contains("bKey")).isTrue();
        assertThat(underTest.contains("cKey")).isTrue();
        underTest.add("dKey");
        assertThat(underTest.contains("aKey")).isFalse();
        assertThat(underTest.contains("bKey")).isTrue();
        assertThat(underTest.contains("cKey")).isTrue();
        assertThat(underTest.contains("dKey")).isTrue();
    }

    @Override
    @Test
    void getTest() {
        assertThat(underTest.get()).isNull();
        underTest.add("aKey");
        assertThat(underTest.get()).isEqualTo("aKey");
        underTest.add("bKey");
        assertThat(underTest.get()).isEqualTo("aKey");
        underTest.add("cKey");
        assertThat(underTest.get()).isEqualTo("aKey");
        underTest.add("dKey");
        assertThat(underTest.get()).isEqualTo("bKey");
    }

    @Override
    @Test
    void containsTest() {
        assertThat(underTest.contains("aKey")).isFalse();
        underTest.add("aKey");
        assertThat(underTest.contains("aKey")).isTrue();
        underTest.add("bKey");
        assertThat(underTest.contains("aKey")).isTrue();
        assertThat(underTest.contains("bKey")).isTrue();
        underTest.add("cKey");
        assertThat(underTest.contains("aKey")).isTrue();
        assertThat(underTest.contains("bKey")).isTrue();
        assertThat(underTest.contains("cKey")).isTrue();
        underTest.add("dKey");
        assertThat(underTest.contains("aKey")).isFalse();
        assertThat(underTest.contains("bKey")).isTrue();
        assertThat(underTest.contains("cKey")).isTrue();
        assertThat(underTest.contains("dKey")).isTrue();
    }

    @Override
    @Test
    void sizeTest() {
        assertThat(underTest.size()).isEqualTo(3);
    }
}
