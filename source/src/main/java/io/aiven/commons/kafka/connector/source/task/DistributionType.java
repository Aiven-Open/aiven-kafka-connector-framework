/*
 * Copyright 2025 Aiven Oy
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
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An enumeration of distribution strategies. */
public enum DistributionType {

  /**
   * Object_Hash takes the context and uses the storage key implementation to get a hash value of
   * the storage key and return a modulus of that relative to the number of maxTasks to decide which
   * task should process a given object
   */
  OBJECT_HASH(
      context -> Optional.of((long) context.getNativeKey().hashCode()),
      "Constructs a hash from the native key and uses that to distribute the "
          + "work across the tasks."),
  /**
   * Partition takes the context and requires the context contain the partition id for it to be able
   * to decide the distribution across the max tasks, using a modulus to ensure even distribution
   * against the configured max tasks
   */
  PARTITION(
      context ->
          context.getPartition().isPresent()
              ? Optional.of((long) context.getPartition().get())https://github.com/Aiven-Open/amqp-connector-for-apache-kafka/pull/11
              : Optional.empty(),
      "Uses the partition value value to distribute the work across the tasks. "
          + "If the partition is undefined the data is skipped."),
  /** ALL accepts all contexts. */
  ALL(
      null,
      "Accepts all input.  This should only be used in cases where there "
          + "is one task or where the backend daa system ensures that only one listener will receive the message");

  /**
   * The function to extract a long value from the context. The long value is then used as the
   * creator in the {@link DistributionType} to generate a value that is used to calculate the task
   * to assign the distribution to.
   */
  private final Function<Context, Optional<Long>> mutation;

  private final String documentation;

  private static final Logger LOGGER = LoggerFactory.getLogger(DistributionType.class);

  /**
   * Creator.
   *
   * @param mutation The mutation required to get the correct details from the context for
   *     distribution.
   * @param documentation The documentation for this distribution type.
   */
  DistributionType(final Function<Context, Optional<Long>> mutation, final String documentation) {
    this.mutation = mutation;
    this.documentation = documentation;
  }

  /**
   * Gets the documentation for the distribution type.
   *
   * @return The documentation for the distribution type.
   */
  public String getDocumentation() {
    return documentation;
  }

  /**
   * Creates a context predicate from this distribution type.
   *
   * @param maxTasks the number of tasks this system is using.
   * @param taskId the TaskID for the distribution.
   * @return a predicate to filter contexts for the specific task.
   */
  public Predicate<Context> asPredicate(int maxTasks, int taskId) {
    return mutation == null
        ? context -> true
        : context -> {
          Optional<Integer> opt =
              mutation.apply(context).map(aLong -> Math.floorMod(Math.abs(aLong), maxTasks));
          if (opt.isPresent()) {
            return opt.get() == taskId;
          }
          LOGGER.warn("SKIPPING INPUT: {} can not determine task for {}", name(), context);
          return false;
        };
  }
}
