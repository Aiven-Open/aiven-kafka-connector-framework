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

/**
 * An enumeration of distribution strategies.
 */
public enum DistributionType {

	/**
	 * Object_Hash takes the context and uses the storage key implementation to get
	 * a hash value of the storage key and return a modulus of that relative to the
	 * number of maxTasks to decide which task should process a given object
	 */
	OBJECT_HASH(context -> context.getStorageKey().isPresent()
			? Optional.of((long) context.getStorageKey().get().hashCode())
			: Optional.empty()),
	/**
	 * Partition takes the context and requires the context contain the partition id
	 * for it to be able to decide the distribution across the max tasks, using a
	 * modulus to ensure even distribution against the configured max tasks
	 */
	PARTITION(context -> context.getPartition().isPresent()
			? Optional.of((long) context.getPartition().get())
			: Optional.empty());

	/**
	 * The function to extract a long value from the context. The long value is then
	 * used as the creator in the {@link DistributionType} to generate a value that
	 * is used to calculate the task to assign the distribution to.
	 */
	private final Function<Context<?>, Optional<Long>> mutation;

	/**
	 * Creator
	 * 
	 * @param mutation
	 *            the mutation required to get the correct details from the context
	 *            for distribution
	 */
	DistributionType(final Function<Context<?>, Optional<Long>> mutation) {
		this.mutation = mutation;
	}

	/**
	 * Returns a configured Distribution Strategy
	 *
	 * @param maxTasks
	 *            the maximum number of configured tasks for this connector
	 *
	 * @return a configured Distribution Strategy with the correct mutation
	 *         configured for proper distribution across tasks of objects being
	 *         processed.
	 */
	public DistributionStrategy getDistributionStrategy(final int maxTasks) {
		return new DistributionStrategy(mutation, maxTasks);
	}
}
