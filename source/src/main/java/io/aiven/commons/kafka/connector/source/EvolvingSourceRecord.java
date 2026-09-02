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

package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.util.io.compression.CompressionType;
import java.io.InputStream;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An evolving source record initially retrieved from the storage layer. */
public final class EvolvingSourceRecord {
  /** the key for the source record */
  private SchemaAndValue keyData;

  /** The value for the source record. */
  private SchemaAndValue valueData;

  /** The headers for this record */
  private Headers headers;

  /** The timestamp for the source record */
  private Long timestamp;

  /** The offset manager entry for this record */
  private OffsetManager.OffsetManagerEntry offsetManagerEntry;

  /** The context associated with this record */
  private final Context context;

  /** The native info for this record. */
  private final AbstractSourceNativeInfo<?, ?> sourceNativeInfo;

  /**
   * Construct a source record from a native item.
   *
   * @param sourceNativeInfo the native information for the native that his record represents.
   * @param offsetManagerEntry the offset manager entry for this record.
   * @param context the context for this record.
   */
  public EvolvingSourceRecord(
      final AbstractSourceNativeInfo<?, ?> sourceNativeInfo,
      final OffsetManager.OffsetManagerEntry offsetManagerEntry,
      final Context context) {
    this.sourceNativeInfo = sourceNativeInfo;
    this.offsetManagerEntry = offsetManagerEntry;
    this.context = context;
  }

  /**
   * Copy constructor for an abstract source record. This constructor makes a copy of the
   * OffsetManagerEntry.
   *
   * @param sourceRecord the source record to copy.
   */
  public EvolvingSourceRecord(final EvolvingSourceRecord sourceRecord) {
    this.sourceNativeInfo = sourceRecord.sourceNativeInfo;
    this.offsetManagerEntry =
        sourceRecord.offsetManagerEntry.fromProperties(
            sourceRecord.getOffsetManagerEntry().getProperties());
    this.keyData = sourceRecord.keyData;
    this.valueData = sourceRecord.valueData;
    this.context = sourceRecord.context;
    this.timestamp = sourceRecord.timestamp;
    this.headers = sourceRecord.headers == null ? null : sourceRecord.headers.duplicate();
  }

  /**
   * Gets the AbstractNativeSource info from the constructor.
   *
   * @param <T> The class that extends AbstractSourceNativeInfo.
   * @return the AbstractNativeSource info from the constructor.
   */
  public <T extends AbstractSourceNativeInfo<?, ?>> T getSourceNativeInfo() {
    return (T) sourceNativeInfo;
  }

  /**
   * Gets then key for the native object.
   *
   * @return The key for the native object.
   */
  public Object getNativeKey() {
    return sourceNativeInfo.nativeKey();
  }

  /**
   * Gets the input stream supplier.
   *
   * @param compressionType The expected compression for the input stream. Resulting stream will be
   *     automatically decompressed.
   * @return the InputStream supplier.
   * @throws UnsupportedOperationException if the sourceNativeInfo does not support input stream.
   */
  public IOSupplier<InputStream> getInputStream(CompressionType compressionType)
      throws UnsupportedOperationException {
    return compressionType.decompress(sourceNativeInfo.getInputStreamSupplier());
  }

  /**
   * Gets the input stream supplier.
   *
   * @return the InputStream supplier.
   * @throws UnsupportedOperationException if the sourceNativeInfo does not support input stream.
   */
  public long estimateInputStreamLength() throws UnsupportedOperationException {
    return sourceNativeInfo.estimateInputStreamLength();
  }

  /**
   * Gets the record count as recorded by the OffsetManager for the native item that this source
   * record is working with.
   *
   * @return The record count as recorded by the OffsetManager for the native item that this source
   *     record is working with.
   */
  public long getRecordCount() {
    return offsetManagerEntry == null ? 0 : offsetManagerEntry.getRecordCount();
  }

  /**
   * Increments the record count as recorded by the OffsetManager for the native item that this
   * source record is working with.
   */
  public void incrementRecordCount() {
    this.offsetManagerEntry.incrementRecordCount();
  }

  /**
   * Sets the key data for this source record.
   *
   * @param keyData The key data for this source record.
   */
  public void setKeyData(final SchemaAndValue keyData) {
    this.keyData = keyData;
  }

  /**
   * Gets the key data for this source record. Makes a defensive copy.
   *
   * @return A copy of the key data for this source record.
   */
  public SchemaAndValue getKey() {
    return new SchemaAndValue(keyData.schema(), keyData.value());
  }

  /**
   * Sets the value data for this source record.
   *
   * @param valueData The key data for this source record.
   */
  public void setValueData(final SchemaAndValue valueData) {
    this.valueData = valueData;
  }

  /**
   * Gets the value data for this source record. Makes a defensive copy.
   *
   * @return A copy of the key data for this source record.
   */
  public SchemaAndValue getValue() {
    return new SchemaAndValue(valueData.schema(), valueData.value());
  }

  /**
   * Set the timestamp for the source record. May be {@code null}, which is the default. Setting to
   * {@code null} will cause Kakfa to set the timestamp to the time it began to process the message.
   *
   * @param timestamp the timestamp.
   */
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Gets the current timestamp for the source record.
   *
   * @return the current timestamp for the source record. May be {@code null}.
   */
  public Long getTimestamp() {
    return timestamp;
  }

  /**
   * Replaces the current headers the {@code headers} header definitions. Makes a copy of the {@code
   * headers} parameter so that any subsequent changes to the {@code headers} parameter are not
   * reflected in this record.
   *
   * @param headers the new headers. May be {@code null}.
   */
  public void setHeaders(final Headers headers) {
    this.headers = headers == null ? null : headers.duplicate();
  }

  /**
   * Gets the current headers. Returns a copy of the headers. Makes a defensive copy.
   *
   * @return the current headers
   */
  public Headers getHeaders() {
    return headers == null ? new ConnectHeaders() : headers.duplicate();
  }

  /**
   * Gets the topic for the source record.
   *
   * @return The topic for the source record or {@code null} if it is not set in the context.
   */
  public String getTopic() {
    return context.getTopic().orElse(null);
  }

  /**
   * Gets the partition for the source record.
   *
   * @return The partition for the source record or {@code null} if it is not set in the context.
   */
  public Integer getPartition() {
    return context.getPartition().orElse(null);
  }

  /**
   * Sets the offset manager entry for this source record.
   *
   * @param offsetManagerEntry The offset manager entry for this source record.
   */
  public void setOffsetManagerEntry(final OffsetManager.OffsetManagerEntry offsetManagerEntry) {
    this.offsetManagerEntry = offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties());
  }

  /**
   * Gets the offset manager entry for this source record. Makes a defensive copy.
   *
   * @return A copy of the offset manager entry for this source record.
   */
  public OffsetManager.OffsetManagerEntry getOffsetManagerEntry() {
    return offsetManagerEntry.fromProperties(
        offsetManagerEntry.getProperties()); // return a defensive copy
  }

  /**
   * Gets the Context for this source record. Makes a defensive copy.
   *
   * @return A copy of the Context for this source record.
   */
  public Context getContext() {
    return new Context(context) {};
  }

  /**
   * Creates a SourceRecord that can be returned to a Kafka topic
   *
   * @param tolerance The error tolerance for the record processing.
   * @param offsetManager The offset manager for the offset entry.
   * @return A kafka {@link SourceRecord SourceRecord} This can return null if error tolerance is
   *     set to 'All'
   */
  public SourceRecord getSourceRecord(
      final ToleranceType tolerance, final OffsetManager offsetManager) {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    try {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Source Record: {} for Topic: {} , Partition: {}, recordCount: {}",
            sourceNativeInfo,
            getTopic(),
            getPartition(),
            getRecordCount());
      }
      offsetManager.addEntry(offsetManagerEntry);
      return new SourceRecord(
          offsetManagerEntry.getManagerKey().getPartitionMap(),
          offsetManagerEntry.getProperties(),
          getTopic(),
          getPartition(),
          keyData.schema(),
          keyData.value(),
          valueData.schema(),
          valueData.value(),
          timestamp,
          headers);
    } catch (DataException e) {
      if (ToleranceType.NONE.equals(tolerance)) {
        throw new ConnectException(
            "Data Exception caught during record to source record transformation", e);
      } else {
        logger.warn(
            "Data Exception caught during record to source record transformation {} . errors.tolerance set to 'all', logging warning and continuing to process.",
            e.getMessage(),
            e);
        return null;
      }
    }
  }
}
