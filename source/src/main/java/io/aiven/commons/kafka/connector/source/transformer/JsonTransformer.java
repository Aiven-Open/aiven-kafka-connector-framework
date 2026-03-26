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

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms an input stream into a number of Jason records. */
public class JsonTransformer extends InputStreamTransformer {

  /**
   * Gets the registry information for this transformer.
   *
   * @return the registry information for this transformer.
   */
  public static TransformerInfo info() {
    return new TransformerInfo(
        "JSONL",
        JsonTransformer.class,
        TransformerInfo.FEATURE_NONE,
        "Parses the input stream as a collection of JSON objects separated by end-of-line characters or the end of the input.  Produces a Kafka record for each JSON object.");
  }

  private final JsonConverter jsonConverter;

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonTransformer.class);

  /**
   * Constructs the JSON Transformer with the specified config.
   *
   * @param config the SourceCommonConfig instance to use.
   */
  public JsonTransformer(final SourceCommonConfig config) {
    super(config, info());
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Map.of("schemas.enable", "false"), false);
  }

  @Override
  public void close() throws Exception {
    jsonConverter.close();
  }

  @Override
  public StreamSpliterator createSpliterator(
      final IOSupplier<InputStream> inputStreamIOSupplier,
      final long streamLength,
      final Context context) {
    return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
      BufferedReader reader;

      @Override
      protected void inputOpened(final InputStream input) {
        reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
      }

      @Override
      public void doClose() {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOGGER.error("Error closing reader: {}", e.getMessage(), e);
          }
        }
      }

      @Override
      public boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
        String line = null;
        try {
          // remove blank and empty lines.
          while (StringUtils.isBlank(line)) {
            line = reader.readLine();
            if (line == null) {
              // end of file
              return false;
            }
          }
          line = line.trim();
          // toConnectData does not actually use topic in the conversion so its fine if it
          // is null.
          action.accept(
              jsonConverter.toConnectData(
                  context.getTopic().orElse(null), line.getBytes(StandardCharsets.UTF_8)));
          return true;
        } catch (IOException e) {
          LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
          return false;
        }
      }
    };
  }
}
