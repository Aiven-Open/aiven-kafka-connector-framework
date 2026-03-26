/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source.task;

import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link DistributionStrategy} provides a mechanism to share the work of processing records from
 * objects (or files) into tasks, which are subsequently processed (potentially in parallel) by
 * Kafka Connect workers.
 */
public final class DistributionStrategy {
  /** the number of tasks to distribute load across */
  private int maxTasks;

  /** the function that takes a {@link Context} and creates a numeric value. */
  private final Function<Context, Optional<Long>> creator;

  /** An undefined distribution strategy */
  public static final int UNDEFINED = -1;

  /**
   * Creates a distribution strategy from a function to convert a context into a numeric value. That
   * value is then distributed across the tasks using a modulus operation.
   *
   * @param creator A function that takes a {@link Context} and creates a numeric value. Function
   *     must be deterministic in that calling the function with the same or duplicate Context will
   *     yield the same result. Result should be positive, negative values are converted using
   *     {@link Math#abs(long)}.
   * @param maxTasks The maximum number of tasks the connector is supporting.
   */
  public DistributionStrategy(final Function<Context, Optional<Long>> creator, final int maxTasks) {
    assertPositiveInteger(maxTasks);
    this.creator = creator;
    this.maxTasks = maxTasks;
  }

  private static void assertPositiveInteger(final int sourceInt) {
    if (sourceInt <= 0) {
      throw new IllegalArgumentException("'maxTasks' must be >= 1.");
    }
  }

  /**
   * Retrieve the taskId that this object should be processed by. Any single object will be assigned
   * deterministically to a single taskId, that will be always return the same taskId output given
   * the same context is used.
   *
   * @param ctx This is the context to derive the taskId from.
   * @return the taskId which this particular task should be assigned to.
   */
  public int getTaskFor(final Context ctx) {
    return creator
        .apply(ctx)
        .map(aLong -> Math.floorMod(Math.abs(aLong), maxTasks))
        .orElse(UNDEFINED);
  }

  /**
   * When a connector receives a reconfigure event this method is called to ensure that the
   * distribution strategy is updated correctly.
   *
   * @param maxTasks The maximum number of tasks created for the Connector
   */
  public void configureDistributionStrategy(final int maxTasks) {
    assertPositiveInteger(maxTasks);
    this.maxTasks = maxTasks;
  }
}
