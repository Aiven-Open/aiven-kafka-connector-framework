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

/** Base class for Lookback test implementations. */
public abstract class AbstractLookbackTest {

  /** Constructor. */
  protected AbstractLookbackTest() {}

  /** Test that adding a key works as expected */
  abstract void addTest();

  /** Test that getting a key works as expected. */
  abstract void getTest();

  /** Test that contains() returns true at the appropriate times. */
  abstract void containsTest();

  /** Test that size() returns the correct value. */
  abstract void sizeTest();
}
