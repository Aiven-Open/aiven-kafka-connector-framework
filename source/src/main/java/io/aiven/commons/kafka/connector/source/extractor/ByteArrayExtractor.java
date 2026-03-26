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

import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// spotless:off
/**
 * ByteArrayExtractor chunks an entire object into a maximum size specified by the extractorBuffer
 * {@link SourceConfigFragment.Setter#extractorBuffer(int)} configuration option.
 *
 * <p>If the configuration option specifies a buffer that is smaller than the length of the input
 * stream, the record will be split into multiple parts. When this happens the extractor makes no
 * guarantees for only once delivery or delivery order as those are dependant upon the Kafka
 * producer and remote consumer configurations. This class will produce the blocks in order and on
 * restart will send any blocks that were not acknowledged by Kafka.
 */
// spotless:on
public class ByteArrayExtractor extends InputStreamExtractor {

  /**
   * Gets the registry information for this extractor.
   *
   * @return the registry information for this extractor.
   */
  public static ExtractorInfo info() {
    return new ExtractorInfo(
        "Bytes",
        ByteArrayExtractor.class,
        ExtractorInfo.FEATURE_NONE,
        "Passes the input stream bytes as Kafka records.  Will split the input stream into multiple records if the"
            + " number of bytes exceeds the specified maximum buffer size.");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayExtractor.class);
  private final int maxBufferSize;

  /**
   * Constructs a ByteArray extractor using the values from the config.
   *
   * @param config the configuration to use.
   */
  public ByteArrayExtractor(SourceCommonConfig config) {
    super(config, info());
    maxBufferSize = config.getExtractorBufferSize();
  }

  @Override
  public StreamSpliterator createSpliterator(
      final IOSupplier<InputStream> inputStreamIOSupplier,
      final long streamLength,
      final Context context) {
    if (streamLength == 0) {
      LOGGER.warn(
          "Object sent for processing has an invalid streamLength of {}, object is empty returning an empty spliterator.",
          streamLength);
      return emptySpliterator(inputStreamIOSupplier);
    }

    return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {

      @Override
      protected void inputOpened(final InputStream input) {}

      @Override
      protected void doClose() {
        // nothing to do.
      }

      @Override
      protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {

        try {
          /// TODO: determine if we need to create a copy of the buffer. I don't see why
          /// --
          /// CW
          final byte[] buffer = new byte[maxBufferSize];
          final byte[] chunk = Arrays.copyOf(buffer, IOUtils.read(inputStream, buffer));
          if (chunk.length > 0) {
            action.accept(new SchemaAndValue(null, chunk));
            return true;
          }

          return false;
        } catch (IOException e) {
          LOGGER.error("Error trying to advance inputStream: {}", e.getMessage(), e);
          return false;
        }
      }
    };
  }

  /**
   * This method returns an empty spliterator when an empty input stream is supplied to be split
   *
   * @param inputStreamIOSupplier The empty input stream that was supplied
   * @return an Empty spliterator to return to the calling method.
   */
  private static StreamSpliterator emptySpliterator(
      final IOSupplier<InputStream> inputStreamIOSupplier) {
    return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
      @Override
      protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
        return false;
      }

      @Override
      protected void doClose() {
        // nothing to do
      }

      @Override
      protected void inputOpened(final InputStream input) {}
    };
  }
}
