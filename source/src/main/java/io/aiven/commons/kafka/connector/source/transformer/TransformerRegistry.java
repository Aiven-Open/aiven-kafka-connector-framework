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
package io.aiven.commons.kafka.connector.source.transformer;

import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A registry of transformers. TransformerRegistry instances are immutable. Use
 * the builder to create them. The Builder will allow multiple registries to be
 * merged into a new registry. Transformer names are not case specific in the
 * registry.
 */
public class TransformerRegistry {
	/** The map of transformer names to info */
	private final Map<String, TransformerInfo> transformers;

	/**
	 * The standard transformers.
	 */
	public static final TransformerRegistry STANDARD = new Builder().add(AvroTransformer.info())
			.add(ByteArrayTransformer.info()).add(CsvTransformer.info()).add(JsonTransformer.info())
			.add(ParquetTransformer.info()).build();

	/**
	 * Creates the builder for a new registry.
	 * 
	 * @return the TransformerRegistry Builder instance.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Creates a registry from a map of names to transformers.
	 * 
	 * @param transformers
	 *            the map of names to transformers.
	 */
	private TransformerRegistry(Map<String, TransformerInfo> transformers) {
		this.transformers = new TreeMap<>(transformers);
	}

	/**
	 * Creates a validator that restricts input to the transformer names in this
	 * registry.
	 *
	 * @return the Validator.
	 */
	public ConfigDef.Validator validator() {
		return ConfigDef.ValidString.in(transformers.keySet().toArray(String[]::new));
	}

	/**
	 * Gets the list of transformer info for this registry.
	 * 
	 * @return the list of TransformerInfo for transformers in this registry.
	 */
	public List<TransformerInfo> list() {
		return new ArrayList<>(transformers.values());
	}

	/**
	 * Gets the list of transformer info that have any of the feature flags
	 * 
	 * @param featureFlags
	 *            one or more features flags "or"ed together.
	 * @return the list of TransformerInfo that contain any of the feature flags.
	 */
	public List<TransformerInfo> anyFeature(int featureFlags) {
		return transformers.values().stream().filter(ti -> ti.anyFeatures(featureFlags)).toList();
	}

	/**
	 * Gets the list of transformer info for this registry that have all the
	 * specified feature flags
	 * 
	 * @param featureFlags
	 *            one or more features flags "or"ed together.
	 * @return the list of TransformerInfo that contain all of the feature flags.
	 */
	public List<TransformerInfo> allFeatures(int featureFlags) {
		return transformers.values().stream().filter(ti -> ti.allFeatures(featureFlags)).toList();
	}

	/**
	 * Gets the TransformerInfo for the specific name.
	 * 
	 * @param name
	 *            the name of the transformer.
	 * @return the TransformerInfo or {@code null} if not found.
	 */
	public TransformerInfo get(String name) {
		return transformers.get(name);
	}

	/**
	 * Gets a TransformerInfo for the specific name. The TransformerInfo with the
	 * first matching name is returned. If multiple names match the string the first
	 * lexically sorted one will be returned.
	 *
	 *
	 * @param name
	 *            the name of the transformer.
	 * @return the TransformerInfo or {@code null} if not found.
	 */
	public TransformerInfo getIgnoreCase(String name) {
		return transformers.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(name)).map(e -> e.getValue())
				.findFirst().orElse(null);
	}

	/**
	 * Gets one of the transformers from the registry.
	 * 
	 * @return one of the transformers or {@code null} if no transformers are
	 *         present.
	 */
	public TransformerInfo any() {
		return transformers.values().stream().findAny().orElse(null);
	}

	/**
	 * A TransformerRegistry builder.
	 */
	public static class Builder {

		private Builder() {
		}

		/** The transformers */
		private final Map<String, TransformerInfo> transformers = new HashMap<>();

		/**
		 * Adds one or more TransformerInfo records to the builder.
		 * 
		 * @param infos
		 *            the TransformerInfo records to add.
		 * @return this
		 */
		public Builder add(TransformerInfo... infos) {
			for (TransformerInfo info : infos) {
				transformers.put(info.commonName(), info);
			}
			return this;
		}

		/**
		 * Adds all the transformers from a registry to the builder. Any existing
		 * TransformersInfos with the same name will be overwritten.
		 * 
		 * @param registry
		 *            the registry to read.
		 * @return this.
		 */
		public Builder add(TransformerRegistry registry) {
			transformers.putAll(registry.transformers);
			return this;
		}

		/**
		 * Build the registry.
		 * 
		 * @return the new Registry.
		 */
		public TransformerRegistry build() {
			return new TransformerRegistry(transformers);
		}
	}
}
