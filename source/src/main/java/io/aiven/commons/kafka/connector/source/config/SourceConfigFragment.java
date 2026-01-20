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

package io.aiven.commons.kafka.connector.source.config;

import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.ConfigFragment;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.source.task.DistributionType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static io.aiven.commons.kafka.connector.source.task.DistributionType.OBJECT_HASH;

/**
 * Defines properties that are shared across all Source implementations.
 */
public final class SourceConfigFragment extends ConfigFragment {

	static final String MAX_POLL_RECORDS = "max.poll.records";
	static final String TARGET_TOPIC = "topic";
	static final String ERRORS_TOLERANCE = "errors.tolerance";
	static final String DISTRIBUTION_TYPE = "distribution.type";

	/** The name of the ring buffer size property */
	static final String RING_BUFFER_SIZE = "ring.buffer.size";
	/** The name of the native start key property */
	public static final String NATIVE_START_KEY = "native.start.key";

	/** The validator for distribution type property */
	static final ConfigDef.Validator DISTRIBUTION_TYPE_VALIDATOR = ConfigDef.CaseInsensitiveValidString
			.in(Arrays.stream(DistributionType.values()).map(Object::toString).toArray(String[]::new));

	/** The validator for errors tolerance property */
	static final ConfigDef.Validator ERRORS_TOLERANCE_VALIDATOR = ConfigDef.CaseInsensitiveValidString
			.in(Arrays.stream(ToleranceType.values()).map(Object::toString).toArray(String[]::new));
	/**
	 * Gets a setter for this fragment.
	 *
	 * @param data
	 *            the data map to modify.
	 * @return the Setter.
	 */
	public static Setter setter(final Map<String, String> data) {
		return new Setter(data);
	}

	/**
	 * Construct the SourceConfigFragment.
	 *
	 * @param dataAccess
	 *            the FragmentDataAccess that this fragment is associated with.
	 */
	public SourceConfigFragment(final FragmentDataAccess dataAccess) {
		super(dataAccess);
	}

	/**
	 * Update the configuration definition with the properties for the source
	 * configuration.
	 * 
	 * @param configDef
	 *            the configuration to update.
	 */
	public static void update(final ConfigDef configDef) {
		configDef.define(RING_BUFFER_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Range.atLeast(1),
				ConfigDef.Importance.MEDIUM, "The number of storage key to store in the ring buffer.");

		configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
				ConfigDef.Importance.MEDIUM, "Max poll records");
		// KIP-298 Error Handling in Connect
		configDef.define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, ToleranceType.NONE.name(),
				ERRORS_TOLERANCE_VALIDATOR, ConfigDef.Importance.MEDIUM,
				"Indicates to the connector what level of exceptions are allowed before the connector stops.");

		configDef.define(TARGET_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
				ConfigDef.Importance.MEDIUM, "The name of the topic to write records to.");
		configDef.define(DISTRIBUTION_TYPE, ConfigDef.Type.STRING, OBJECT_HASH.name(), DISTRIBUTION_TYPE_VALIDATOR,
				ConfigDef.Importance.MEDIUM,
				"Based on tasks.max config and the type of strategy selected, objects are processed in distributed"
						+ " way by Kafka connect workers.");

		// TODO FIX ME this should be updated to add 'since version 3.4.2' when
		// ExtendedConfigKey is used.
		configDef.define(NATIVE_START_KEY, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM,
				"An identifier for the source connector to know which key to start processing from, on a restart it will also begin reading messages from this point as well. Available since 3.4.2");
	}

	/**
	 * Gets the target topic.
	 *
	 * @return the target topic.
	 */
	public String getTargetTopic() {
		return getString(TARGET_TOPIC);
	}

	/**
	 * Gets the maximum number of records to poll at one time.
	 *
	 * @return The maximum number of records to poll at one time.
	 */
	public int getMaxPollRecords() {
		return getInt(MAX_POLL_RECORDS);
	}

	/**
	 * Gets the error tolerance.
	 *
	 * @return the error tolerance.
	 */
	public ToleranceType getErrorsTolerance() {
		return ToleranceType.valueOf(getString(ERRORS_TOLERANCE).toUpperCase(Locale.ROOT));
	}

	/**
	 * Gets the distribution type
	 *
	 * @return the distribution type.
	 */
	public DistributionType getDistributionType() {
		return DistributionType.valueOf(getString(DISTRIBUTION_TYPE).toUpperCase(Locale.ROOT));
	}

	/**
	 * Gets the ring buffer size.
	 *
	 * @return the ring buffer size.
	 */
	public int getRingBufferSize() {
		return getInt(RING_BUFFER_SIZE);
	}

	/**
	 * Gets the nativeStartKey.
	 *
	 * @return the key to start consuming records from.
	 */
	public String getNativeStartKey() {
		return getString(NATIVE_START_KEY);
	}

	/**
	 * The SourceConfigFragment setter.
	 */
	public static class Setter extends AbstractFragmentSetter<Setter> {
		/**
		 * Constructor.
		 *
		 * @param data
		 *            the data to modify.
		 */
		protected Setter(final Map<String, String> data) {
			super(data);
		}

		/**
		 * Set the maximum poll records.
		 *
		 * @param maxPollRecords
		 *            the maximum number of records to poll.
		 * @return this
		 */
		public Setter maxPollRecords(final int maxPollRecords) {
			return setValue(MAX_POLL_RECORDS, maxPollRecords);
		}

		/**
		 * Sets the error tolerance.
		 *
		 * @param tolerance
		 *            the error tolerance
		 * @return this.
		 */
		public Setter errorsTolerance(final ToleranceType tolerance) {
			return setValue(ERRORS_TOLERANCE, tolerance.name());
		}

		/**
		 * Sets the target topic.
		 *
		 * @param targetTopic
		 *            the target topic.
		 * @return this.
		 */
		public Setter targetTopic(final String targetTopic) {
			return setValue(TARGET_TOPIC, targetTopic);
		}

		/**
		 * Sets the distribution type.
		 *
		 * @param distributionType
		 *            the distribution type.
		 * @return this
		 */
		public Setter distributionType(final DistributionType distributionType) {
			return setValue(DISTRIBUTION_TYPE, distributionType.name());
		}

		/**
		 * Sets the ring buffer size.
		 *
		 * @param ringBufferSize
		 *            the ring buffer size
		 * @return this.
		 */
		public Setter ringBufferSize(final int ringBufferSize) {
			return setValue(RING_BUFFER_SIZE, ringBufferSize);
		}

		/**
		 * Sets the initial native key to start from.
		 *
		 * @param nativeStartKey
		 *            the key to start reading new messages from.
		 * @return this.
		 */
		public Setter nativeStartKey(final String nativeStartKey) {
			return setValue(NATIVE_START_KEY, nativeStartKey);
		}
	}
}
