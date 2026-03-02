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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NoneTest extends AbstractLookbackTest {

    private final None<String> underTest = new None<>();

    @Override
    @Test
    void addTest() {
        underTest.add("aKey");
        assertThat(underTest.contains("aKey")).as("Should never contain a key").isFalse();
    }


    @Override
    @Test
    void getTest() {
        assertThat(underTest.get()).as("Should never contain a key").isNull();
        underTest.add("aKey");
        assertThat(underTest.get()).as("Should never contain a key").isNull();
    }


    @Override
    @Test
    void containsTest() {
        assertThat(underTest.contains("aKey")).as("Should never contain a key").isFalse();
        underTest.add("aKey");
        assertThat(underTest.contains("aKey")).as("Should never contain a key").isFalse();
    }

    @Override
    @Test
    void sizeTest() {
        assertThat(underTest.size()).isEqualTo(0);
        underTest.add("aKey");
        assertThat(underTest.size()).isEqualTo(0);
    }

}
