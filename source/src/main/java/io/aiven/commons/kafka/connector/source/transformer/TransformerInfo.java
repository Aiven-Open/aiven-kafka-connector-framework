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

/**
 * The Information about the transformer.
 *
 * @param commonName
 *            the common name for the Transformer. Used in configurations.
 * @param transformerClass
 *            the Transformer class.
 * @param featureFlags
 *            A set of feature flags "or"ed together.
 * @param description
 *            A description of this transformer.
 */
public record TransformerInfo(String commonName, Class<? extends Transformer> transformerClass, int featureFlags,
		String description) {
	/**
	 * Feature flag signifying: No additional features. Used in constructor. Should
	 * not be used for test lack of features. Use {@link #noFeatures()} instead.
	 */
	public final static int FEATURE_NONE = 0;
	/**
	 * Feature flag signifying: Transformer handles compression internally.
	 */
	public final static int FEATURE_INTERNAL_COMPRESSION = 1;
	/**
	 * Offset shift for private features. Transformers defined in other packages may
	 * define up to 8 additional features numbered (0 - 7), The feature flags for
	 * those features should be calculated as =
	 * {@code 1<<(PRIVATE_FEATURE_SHIFT + number)}
	 *
	 */
	public final static int PRIVATE_FEATURE_SHIFT = 24;

	/**
	 * Determines if the transformer supports all the features.
	 *
	 * @param featureFlags
	 *            one or more features "or"ed together.
	 * @return {@code true} if the transformer supports all the features.
	 */
	public boolean allFeatures(int featureFlags) {
		return (featureFlags & this.featureFlags) == featureFlags;
	}

	/**
	 * Determines if the transformer supports any of the features.
	 *
	 * @param featureFlags
	 *            one or more features "or"ed together.
	 * @return {@code true} if the transformer supports any the features.
	 */
	public boolean anyFeatures(int featureFlags) {
		return (featureFlags & this.featureFlags) != 0;
	}

	/**
	 * Determines if the transformer supports no additional features.
	 *
	 * @return {@code true} if the transformer does not support any additional
	 *         features.
	 */
	public boolean noFeatures() {
		return featureFlags == FEATURE_NONE;
	}
}
