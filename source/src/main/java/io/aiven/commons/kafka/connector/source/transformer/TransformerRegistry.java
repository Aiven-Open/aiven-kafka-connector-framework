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
package io.aiven.commons.kafka.connector.source.transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * A registry of transformers. TransformerRegistry instances are immutable.  Use the builder to create them.  The
 * Builder will allow multiple registries to be merged into a new registry.
 * Transformer names are not case specific in the registry.
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
	 * Gets the list of transformer info for this registry.
	 * 
	 * @return the list of TransformerInfo for transformers in this registry.
	 */
	public List<TransformerInfo> list() {
		return new ArrayList<>(transformers.values());
	}

	/**
	 * Gets the Transformer info for the specific name.
	 * 
	 * @param name
	 *            the name of the transformer.
	 * @return the TransformerInfo or {@code null} if not found.
	 */
	public TransformerInfo get(String name) {
		return transformers.get(name.toUpperCase(Locale.ROOT));
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
	 * The TransformerInfo record
	 * 
	 * @param commonName
	 *            the common name for the class.
	 * @param transformerClass
	 *            the class.
	 * @param requiresInputStream
	 *            true if the transformer requires/supports input stream.
	 */
	public record TransformerInfo(String commonName, Class<? extends Transformer> transformerClass,
			boolean requiresInputStream) {
	}

	/**
	 * A TransformerRegistry builder.
	 */
	public static class Builder {
		/** The transformers */
		private final Map<String, TransformerInfo> transformers = new HashMap<>();

		/**
		 * Adds a TransformerInfo to the builder.
		 * 
		 * @param info
		 *            the info to add.
		 * @return this
		 */
		public Builder add(TransformerInfo info) {
			transformers.put(info.commonName.toUpperCase(Locale.ROOT), info);
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
