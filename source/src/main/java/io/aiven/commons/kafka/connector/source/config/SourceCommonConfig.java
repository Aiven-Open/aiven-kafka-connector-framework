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

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;

import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfig;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigDef;
import io.aiven.commons.kafka.connector.source.task.DistributionType;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.util.List;
import java.util.Map;

/**
 * The common definitions for source connectors.
 */
public class SourceCommonConfig extends ConnectorCommonConfig {

	/**
	 * The standard source configuration fragment.
	 */
	private final SourceConfigFragment sourceConfigFragment;

	/**
	 * Constructor.
	 *
	 * @param definition
	 *            the configuration definition.
	 * @param originals
	 *            the initial configuration data.
	 */
	public SourceCommonConfig(final SourceCommonConfigDef definition, final Map<String, String> originals) {
		super(definition, originals);
		final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
		sourceConfigFragment = new SourceConfigFragment(dataAccess);
	}

	/**
	 * Gets the target topic to write messages to.
	 *
	 * @return the target topic to write messages to.
	 */
	public String getTargetTopic() {
		return sourceConfigFragment.getTargetTopic();
	}

	/**
	 * Gets the tolerance for errors.
	 *
	 * @return the tolerance for errors.
	 */
	public ToleranceType getErrorsTolerance() {
		return sourceConfigFragment.getErrorsTolerance();
	}

	/**
	 * Gets the distribution type.
	 *
	 * @return the distribution type.
	 */
	public DistributionType getDistributionType() {
		return sourceConfigFragment.getDistributionType();
	}

	/**
	 * Gets the maximum number of records to return in a single poll.
	 *
	 * @return the maximum number of records to return in a single poll.
	 */
	public int getMaxPollRecords() {
		return sourceConfigFragment.getMaxPollRecords();
	}

	/**
	 * Gets the native start key for the initial file to scan.
	 *
	 * @return the native start key.
	 */
	public String getNativeStartKey() {
		return sourceConfigFragment.getNativeStartKey();
	}

	/**
	 * Gets the size of the ring buffer used to track read files.
	 *
	 * @return the size of the ring buffer.
	 */
	public int getRingBufferSize() {
		return sourceConfigFragment.getRingBufferSize();
	}

	/**
	 * Gets the transformer defined for this source.
	 * 
	 * @return the Transformer defined for this source.
	 */
	public Transformer getTransformer() {
		return sourceConfigFragment.getTransformer(this);
	}
	/**
	 * Gets the size of the Transformer buffer if the transformer builds a buffered
	 * input stream.
	 * 
	 * @return the size of the transformer buffer in bytes.
	 */
	public int getTransformerBufferSize() {
		return sourceConfigFragment.getTransformerBufferSize();
	}

	/**
	 * Gets the size of the Transformer cache if the transformer supports a cache.
	 *
	 * @return the size of the transformer cache in bytes.
	 */
	public int getTransformerCacheSize() {
		return sourceConfigFragment.getTransformerCacheSize();
	}

	/**
	 * Gets the CSV header enabled flag.
	 * 
	 * @return {@code true} if headers should be extracted from the CSV input,
	 *         {@code false} otherwise.
	 */
	public boolean isCsvTransformerHeaderEnabled() {
		return sourceConfigFragment.isCsvTransformerHeaderEnabled();
	}

	/**
	 * Gets the specified headers, if any, for the CSV transformer.
	 * 
	 * @return list of headers specified for the CSV transformer.
	 */
	public List<String> getCsvTransformerHeader() {
		return sourceConfigFragment.getCsvTransformerHeader();
	}

	/**
	 * The common source configuration definition.
	 */
	public static class SourceCommonConfigDef extends ConnectorCommonConfigDef {

		/**
		 * Constructor.
		 */
		public SourceCommonConfigDef() {
			super();
			SourceConfigFragment.update(this);
		}

		@Override
		public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
			final Map<String, ConfigValue> values = super.multiValidate(valueMap);
			final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
			new SourceConfigFragment(fragmentDataAccess).validate(values);
			return values;
		}
	}
}
