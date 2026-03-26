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

import static io.aiven.commons.kafka.connector.source.transformer.TransformerInfo.FEATURE_INTERNAL_COMPRESSION;

import io.aiven.commons.kafka.connector.source.AbstractSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.util.io.compression.CompressionType;
import java.io.IOException;
import java.io.InputStream;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;

/**
 * Extracts data from the EvolvingSourceRecord to a Key SchemaAndValue object and a stream of
 * SchemaAndValue objects for the values.
 *
 * <p>This class is used within the EvolvingSourceRecordIterator to convert the EvolvingSourceRecord
 * into one or more Kafka source records.
 *
 * <p>This implementation of Transformer assumes that the SourceRecord supports returning an
 * inputStream.
 *
 * <p>Developers should use this class when the format being transformed supports reading 1 internal
 * record from the stream at a time. If this is not the case it is more efficient to write a
 * transformer that directly implements generate records.
 */
public abstract class InputStreamTransformer extends Transformer {

  /**
   * Constructs a transformer that processes an input stream rather than a stream of objects.
   *
   * @param config The SourceCommonsConfig implementation.
   * @param info The TransformerInfo for this transformer.
   */
  protected InputStreamTransformer(SourceCommonConfig config, TransformerInfo info) {
    super(config, info);
  }

  /**
   * Gets a stream of SchemaAndValue records from the input stream.
   *
   * @param sourceRecord The EvolvingSourceRecord being processed.
   * @return the stream of values for Kafka SourceRecords.
   */
  @Override
  public Stream<SchemaAndValue> generateRecords(final EvolvingSourceRecord sourceRecord) {
    IOSupplier<InputStream> inputStreamSupplier =
        info.allFeatures(FEATURE_INTERNAL_COMPRESSION)
            ? sourceRecord.getInputStream(CompressionType.NONE)
            : sourceRecord.getInputStream(config.getCompressionType());
    final StreamSpliterator spliterator =
        createSpliterator(
            inputStreamSupplier,
            sourceRecord.estimateInputStreamLength(),
            sourceRecord.getContext());
    return StreamSupport.stream(spliterator, false)
        .onClose(spliterator::close)
        .skip(sourceRecord.getRecordCount());
  }

  /**
   * Creates the stream spliterator for this transformer.
   *
   * @param inputStreamIOSupplier the input stream supplier.
   * @param streamLength the length of the input stream, which id defined in the
   *     AbstractSourceNativeInfo {@link AbstractSourceNativeInfo#UNKNOWN_STREAM_LENGTH} may be used
   *     to specify a stream with an unknown length, streams of length zero will log an error and
   *     return an empty stream
   * @param context the context
   * @return a StreamSpliterator instance.
   */
  protected abstract StreamSpliterator createSpliterator(
      IOSupplier<InputStream> inputStreamIOSupplier, long streamLength, Context context);

  /** A Spliterator that performs various checks on the opening/closing of the input stream. */
  public abstract static class StreamSpliterator implements Spliterator<SchemaAndValue> {
    /** The input stream supplier. */
    private final IOSupplier<InputStream> inputStreamIOSupplier;

    /**
     * The logger to be used by all instances of this class. This will be the Transformer logger.
     */
    protected final Logger logger;

    /**
     * * The input stream. Will be null until {@link #inputOpened} has completed. May be used for
     * reading but should not be closed or otherwise made unreadable.
     */
    protected InputStream inputStream;

    /** A flag indicate that the input stream has been closed. */
    private boolean closed;

    /**
     * Constructor.
     *
     * @param logger The logger for this Spliterator to use.
     * @param inputStreamIOSupplier the InputStream supplier
     */
    protected StreamSpliterator(
        final Logger logger, final IOSupplier<InputStream> inputStreamIOSupplier) {
      this.logger = logger;
      this.inputStreamIOSupplier = inputStreamIOSupplier;
    }

    /**
     * Attempt to read the next record. If there is no record to read or an error occurred return
     * false. If a record was created, call {@code action.accept()} with the record.
     *
     * @param action the Consumer to call if record is created.
     * @return {@code true} if a record was processed, {@code false} otherwise.
     */
    protected abstract boolean doAdvance(Consumer<? super SchemaAndValue> action);

    /** Method to close additional inputs if needed. */
    protected abstract void doClose();

    /**
     * Closes the StreamSplitter by calling {@link #doClose()} and then closing the input stream.
     */
    public final void close() {
      doClose();
      try {
        if (inputStream != null) {
          inputStream.close();
          inputStream = null; // NOPMD setting null to release resources
          closed = true;
        }
      } catch (IOException e) {
        logger.error("Error trying to close inputStream: {}", e.getMessage(), e);
      }
    }

    /**
     * Allows modification of input stream. Called immediately after the input stream is opened.
     * Implementations may modify the type of input stream by wrapping it with a specific
     * implementation, or may create Readers from the input stream. The modified input stream must
     * be returned. If a Reader or similar class is created from the input stream the input stream
     * must be returned. The {@link #inputStream} instance variable will be null until {@code
     * inputOpened} has completed. The implementation of the interface is responsible for closing
     * any newly constructed readers or input streams in the {@link #doClose()} method.
     *
     * @param input the input stream that was just opened.
     * @throws IOException on IO error.
     */
    protected abstract void inputOpened(InputStream input) throws IOException;

    @Override
    public final boolean tryAdvance(final Consumer<? super SchemaAndValue> action) {
      if (closed) {
        return false;
      }
      boolean result = false;
      try {
        if (inputStream == null) {
          try {
            inputStream = inputStreamIOSupplier.get();
            inputOpened(inputStream);
          } catch (IOException e) {
            logger.error("Error trying to open inputStream: {}", e.getMessage(), e);
            close();
            return false;
          }
        }
        result = doAdvance(action);
      } catch (RuntimeException e) {
        logger.error("Error trying to advance data: {}", e.getMessage(), e);
      }
      if (!result) {
        close();
      }
      return result;
    }

    @Override
    public final Spliterator<SchemaAndValue> trySplit() { // NOPMD returning null is reqruied by API
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return ORDERED | NONNULL;
    }
  }
}
