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
package io.aiven.commons.kafka.connector.source.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.config.ConfigDef;

/**
 * A registry of extractors. ExtractorRegistry instances are immutable. Use the builder to
 * create them. The Builder will allow multiple registries to be merged into a new registry.
 * Extractor names are not case specific in the registry.
 */
public class ExtractorRegistry {
  /** The map of extractor names to info */
  private final Map<String, ExtractorInfo> extractors;

  /** The standard extractors. */
  public static final ExtractorRegistry STANDARD =
      new Builder()
          .add(AvroExtractor.info())
          .add(ByteArrayExtractor.info())
          .add(CsvExtractor.info())
          .add(JsonExtractor.info())
          .add(ParquetExtractor.info())
          .build();

  /**
   * Creates the builder for a new registry.
   *
   * @return the ExtractorRegistry Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a registry from a map of names to extractors.
   *
   * @param extractors the map of names to extractors.
   */
  private ExtractorRegistry(Map<String, ExtractorInfo> extractors) {
    this.extractors = new TreeMap<>(extractors);
  }

  /**
   * Creates a validator that restricts input to the extractor names in this registry.
   *
   * @return the Validator.
   */
  public ConfigDef.Validator validator() {
    return ConfigDef.ValidString.in(extractors.keySet().toArray(String[]::new));
  }

  /**
   * Gets the list of extractor info for this registry.
   *
   * @return the list of ExtractorInfo for extractors in this registry.
   */
  public List<ExtractorInfo> list() {
    return new ArrayList<>(extractors.values());
  }

  /**
   * Gets the list of extractor info that have any of the feature flags
   *
   * @param featureFlags one or more features flags "or"ed together.
   * @return the list of ExtractorInfo that contain any of the feature flags.
   */
  public List<ExtractorInfo> anyFeature(int featureFlags) {
    return extractors.values().stream().filter(ti -> ti.anyFeatures(featureFlags)).toList();
  }

  /**
   * Gets the list of extractor info for this registry that have all the specified feature flags
   *
   * @param featureFlags one or more features flags "or"ed together.
   * @return the list of ExtractorInfo that contain all of the feature flags.
   */
  public List<ExtractorInfo> allFeatures(int featureFlags) {
    return extractors.values().stream().filter(ti -> ti.allFeatures(featureFlags)).toList();
  }

  /**
   * Gets the ExtractorInfo for the specific name.
   *
   * @param name the name of the extractor.
   * @return the ExtractorInfo or {@code null} if not found.
   */
  public ExtractorInfo get(String name) {
    return extractors.get(name);
  }

  /**
   * Gets a ExtractorInfo for the specific name. The ExtractorInfo with the first matching name
   * is returned. If multiple names match the string the first lexically sorted one will be
   * returned.
   *
   * @param name the name of the extractor.
   * @return the ExtractorInfo or {@code null} if not found.
   */
  public ExtractorInfo getIgnoreCase(String name) {
    return extractors.entrySet().stream()
        .filter(e -> e.getKey().equalsIgnoreCase(name))
        .map(e -> e.getValue())
        .findFirst()
        .orElse(null);
  }

  /**
   * Gets one of the extractors from the registry.
   *
   * @return one of the extractors or {@code null} if no extractors are present.
   */
  public ExtractorInfo any() {
    return extractors.values().stream().findAny().orElse(null);
  }

  /** A ExtractorRegistry builder. */
  public static class Builder {

    private Builder() {}

    /** The extractors */
    private final Map<String, ExtractorInfo> extractors = new HashMap<>();

    /**
     * Adds one or more ExtractorInfo records to the builder.
     *
     * @param infos the ExtractorInfo records to add.
     * @return this
     */
    public Builder add(ExtractorInfo... infos) {
      for (ExtractorInfo info : infos) {
        extractors.put(info.commonName(), info);
      }
      return this;
    }

    /**
     * Adds all the extractors from a registry to the builder. Any existing ExtractorsInfos with
     * the same name will be overwritten.
     *
     * @param registry the registry to read.
     * @return this.
     */
    public Builder add(ExtractorRegistry registry) {
      extractors.putAll(registry.extractors);
      return this;
    }

    /**
     * Build the registry.
     *
     * @return the new Registry.
     */
    public ExtractorRegistry build() {
      return new ExtractorRegistry(extractors);
    }
  }
}
