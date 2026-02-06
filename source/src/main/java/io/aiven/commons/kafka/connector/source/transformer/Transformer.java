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

import io.aiven.commons.kafka.connector.source.AbstractSourceRecord;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.stream.Stream;

/**
 * Extracts data from the native abstract source record to Key SchemaAndValue
 * object and a stream of SchemaAndValue objects for the values.
 *
 * This class is used within the AbstractSourceRecordIterator to convert the
 * abstract source record into one or more Kafka source records.
 *
 */
public abstract class Transformer implements AutoCloseable {
	/** The source configuration for the Kafka task. */
	protected final SourceCommonConfig config;

	/**
	 * Constructor.
	 * 
	 * @param config
	 *            The configuration for the source connector.
	 */
	protected Transformer(SourceCommonConfig config) {
		this.config = config;
	}
	/**
	 * Gets a stream of SchemaAndValue records from the input stream.
	 * 
	 * @param nativeSourceData
	 *            The native source data.
	 * @param sourceRecord
	 *            The AbstractSourceRecord being processed.
	 * 
	 * @param <T>
	 *            The concrete class of the AbstractSourceRecord.
	 * @return the stream of values for Kafka SourceRecords.
	 */
	public abstract <T extends AbstractSourceRecord<?, ?, ?, T>> Stream<SchemaAndValue> generateRecords(
			final NativeSourceData<?, ?, ?, T> nativeSourceData, final T sourceRecord);

	/**
	 * Convert the native key into a Schema and Value for Kafka.
	 * 
	 * @param abstractSourceRecord
	 *            the Abstract source record to extract the keyData from.
	 * 
	 * @return a SchemaAndValue for the key.
	 */
	public abstract SchemaAndValue generateKeyData(final AbstractSourceRecord<?, ?, ?, ?> abstractSourceRecord);

	@Override
	public void close() throws Exception {
		// do nothing.
	}
}
