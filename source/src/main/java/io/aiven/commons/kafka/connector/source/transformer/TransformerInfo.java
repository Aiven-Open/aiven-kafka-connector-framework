package io.aiven.commons.kafka.connector.source.transformer;

/**
 * The Information about the transformer.
 *
 * @param commonName       the common name for the Transformer.  Used in configurations.
 * @param transformerClass the Transformer class.
 * @param featureFlags     A set of feature flags "or"ed together.
 */
public record TransformerInfo(String commonName, Class<? extends Transformer> transformerClass,
                              int featureFlags) {
    /**
     * Feature flag signifying: No additional features.  Used in constructor.  Should not be used for test lack of
     * features.  Use {@link #noFeatures()} instead.
     */
    public final static int FEATURE_NONE = 0;
    /**
     * Feature flag signifying: Transformer handles compression internally.
     */
    public final static int FEATURE_INTERNAL_COMPRESSION = 1;
    /**
     * Offset shift for private features.  Transformers defined in other packages may define up to 8 additional features
     * numbered (0 - 7),  The feature flags for those features should be calculated as = {@code 1<<(PRIVATE_FEATURE_SHIFT + number)}
     *
     */
    public final static int PRIVATE_FEATURE_SHIFT = 24;

    /**
     * Determines if the transformer supports all the features.
     *
     * @param featureFlags one or more features "or"ed together.
     * @return {@code true} if the transformer supports all the features.
     */
    public boolean allFeatures(int featureFlags) {
        return (featureFlags & this.featureFlags) == featureFlags;
    }

    /**
     * Determines if the transformer supports any of the features.
     *
     * @param featureFlags one or more features "or"ed together.
     * @return {@code true} if the transformer supports any the features.
     */
    public boolean anyFeatures(int featureFlags) {
        return (featureFlags & this.featureFlags) != 0;
    }

    /**
     * Determines if the transformer supports no additional features.
     *
     * @return {@code true} if the transformer does not support any additional features.
     */
    public boolean noFeatures() {
        return featureFlags == FEATURE_NONE;
    }
}
