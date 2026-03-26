/*
 * Copyright 2026 Aiven Oy
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

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.kafka.connector.source.AbstractSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import io.aiven.commons.kafka.connector.source.impl.ExampleSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.impl.nativeProvided.ExampleNativeItem;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.util.io.compression.CompressionType;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Base test for transformers that consume {@code IOSource<InputStream>} objects. */
public abstract class IOTransformerTest {

  /** The transformer under test. */
  protected Transformer transformer;

  /**
   * Setup the transformer for testing.
   *
   * @return a configured Transformer.
   */
  protected abstract Transformer setupTransformer(CompressionType compressionType);

  /**
   * Generate one buffer for the transformer to read. This must be a valid data buffer for the
   * Transformer under test.
   *
   * @return the buffer to read.
   * @throws IOException on generation error.
   */
  protected abstract byte[] generateOneBuffer() throws IOException;

  /**
   * Creates an EvolvingSourceRecord with the Context of OffsetManagerEntry as would be set by the
   * EvolvingSourceRecordIterator processes.
   *
   * @param nativeItem the native Item to create the recrod for
   * @return the populated native Item.
   */
  protected EvolvingSourceRecord createExampleSourceRecord(
      AbstractSourceNativeInfo<?, ?> nativeItem) {
    return new EvolvingSourceRecord(
        nativeItem,
        new ExampleOffsetManagerEntry((String) nativeItem.nativeKey(), "group1"),
        new Context(nativeItem.nativeKey()));
  }

  @BeforeEach
  final void setUp() {
    transformer = setupTransformer(CompressionType.NONE);
  }

  @AfterEach
  final void teardown() throws Exception {
    transformer.close();
  }

  @Test
  void verifyInfo() {
    assertThat(transformer.info.transformerClass()).isEqualTo(transformer.getClass());
  }

  /**
   * Verifies that properly formed data can be read.
   *
   * @throws Exception on error
   */
  @Test
  abstract void testReadData() throws Exception;

  /**
   * Verifies that output records that are extracted from the data can be skipped. For example a
   * JSONL transformer returns one record for every line in the JSONL structure. The transformer
   * must be able to start returning output from some point after the start of the structure
   *
   * @throws Exception on error.
   */
  @Test
  abstract void testReadRecordsSkipFew() throws Exception;

  /**
   * Verifies that output attempting to skip more records than are embedded in the structure does
   * not fail.
   *
   * @throws Exception on error.
   */
  @Test
  abstract void testReadRecordsSkipMoreRecordsThanExist() throws Exception;

  /**
   * Verifies that an IOException thrown when the IOSupplier is retrieved does not cause the
   * Transformer to abort.
   */
  @Test
  final void testIOExceptionDuringCreation() throws IOException {
    final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
    final ExampleSourceNativeInfo nativeSourceInfo =
        new ExampleSourceNativeInfo(nativeItem) {
          @Override
          public InputStream getInputStream() throws IOException {
            throw new IOException("Exception reading data stream");
          }
        };

    final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(nativeSourceInfo);

    final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

    assertThat(records).isEmpty();
  }

  /**
   * Verifies that an IOException during the InputStream read does not cause the Transformer to
   * abort.
   */
  @Test
  final void testIOExceptionDuringDataRead() throws IOException {
    final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
    final ExampleSourceNativeInfo nativeSourceInfo =
        new ExampleSourceNativeInfo(nativeItem) {
          @Override
          public InputStream getInputStream() {
            return new InputStream() {
              @Override
              public int read() throws IOException {
                throw new IOException("Exception reading data stream");
              }
            };
          }
        };

    final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(nativeSourceInfo);
    final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);
    assertThat(records).isEmpty();
  }

  /** Verifies that an empty InputStream does not cause Transformer failure. */
  @Test
  final void testGetRecordsEmptyInputStream() throws IOException {
    final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", new byte[0]);
    ExampleSourceNativeInfo nativeSourceInfo = new ExampleSourceNativeInfo(nativeItem);
    final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(nativeSourceInfo);

    final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

    assertThat(records).isEmpty();
  }

  /**
   * Verifies that close is call after the records are processed.
   *
   * @throws IOException on error.
   */
  @Test
  final void verifyCloseCalledAtEnd() throws IOException {
    final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateOneBuffer());
    final CloseTrackingStream[] ctsRef = new CloseTrackingStream[1];
    ExampleSourceNativeInfo nativeSourceInfo =
        new ExampleSourceNativeInfo(nativeItem) {
          @Override
          public InputStream getInputStream() throws IOException {
            ctsRef[0] = new CloseTrackingStream(super.getInputStream());
            return ctsRef[0];
          }
        };
    final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(nativeSourceInfo);

    final Stream<SchemaAndValue> records = transformer.generateRecords(sourceRecord);

    assertThat(records.count()).isGreaterThan(0);
    assertThat(ctsRef[0].closeCount).isGreaterThan(0);
  }

  /**
   * Verifies that close is called after the iterator finished.
   *
   * @throws IOException on error.
   */
  @Test
  void verifyCloseCalledAtIteratorEnd() throws IOException {
    final ExampleNativeItem nativeItem = new ExampleNativeItem("nativeKey", generateOneBuffer());
    final CloseTrackingStream[] ctsRef = new CloseTrackingStream[1];
    ExampleSourceNativeInfo nativeSourceInfo =
        new ExampleSourceNativeInfo(nativeItem) {
          @Override
          public InputStream getInputStream() throws IOException {
            ctsRef[0] = new CloseTrackingStream(super.getInputStream());
            return ctsRef[0];
          }
        };

    final EvolvingSourceRecord sourceRecord = createExampleSourceRecord(nativeSourceInfo);

    final Iterator<SchemaAndValue> records = transformer.generateRecords(sourceRecord).iterator();
    while (records.hasNext()) {
      records.next();
    }

    assertThat(ctsRef[0].closeCount).isGreaterThan(0);
  }

  /** A class to track that an input stream has been closed. */
  private static class CloseTrackingStream extends InputStream {
    InputStream delegate;
    int closeCount;

    CloseTrackingStream(final InputStream stream) {
      super();
      this.delegate = stream;
    }

    @Override
    public int read() throws IOException {
      if (closeCount > 0) {
        throw new IOException("ERROR Read after close");
      }
      return delegate.read();
    }

    @Override
    public void close() throws IOException {
      closeCount++;
      delegate.close();
    }
  }
}
