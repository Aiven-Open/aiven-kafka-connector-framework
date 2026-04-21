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
package io.aiven.commons.kafka.connector.source.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

public class DistributionTypeTest {

  @Test
  void partitionTest() {
    Predicate<Context> predicate = DistributionType.PARTITION.asPredicate(2, 0);
    Context ctxt = new Context("The Key");
    assertThat(predicate.test(ctxt)).isFalse();
    ctxt.setPartition(1);
    assertThat(predicate.test(ctxt)).isFalse();
    ctxt.setPartition(2);
    assertThat(predicate.test(ctxt)).isTrue();
  }

  @Test
  void hashTest() {
    // "The Key" has a hash code of 312633840
    // "The Key3" has a has code of 1101714499
    Predicate<Context> predicate = DistributionType.OBJECT_HASH.asPredicate(2, 0);
    Context ctxt = new Context("The Key");
    System.out.println("The Key".hashCode());
    assertThat(predicate.test(ctxt)).isTrue();
    ctxt = new Context("The Key3");
    assertThat(predicate.test(ctxt)).isFalse();

    predicate = DistributionType.OBJECT_HASH.asPredicate(2, 1);
    ctxt = new Context("The Key");
    assertThat(predicate.test(ctxt)).isFalse();
    ctxt = new Context("The Key3");
    assertThat(predicate.test(ctxt)).isTrue();

    predicate = DistributionType.OBJECT_HASH.asPredicate(2, 3);
    ctxt = new Context("The Key");
    assertThat(predicate.test(ctxt)).isFalse();
    ctxt = new Context("The Key3");
    assertThat(predicate.test(ctxt)).isFalse();
  }
}
