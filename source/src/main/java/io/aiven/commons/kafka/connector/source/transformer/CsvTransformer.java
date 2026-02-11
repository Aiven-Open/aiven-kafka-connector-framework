/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.commons.kafka.connector.source.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * This class provides a transformer to take a List of CSVRecord generated from
 * apache csv-commons and transforms that data into a SchemaAndValue Object
 * usable by Connect to add messages to Kafka.
 */
public class CsvTransformer extends InputStreamTransformer {
	private static final Logger LOGGER = LoggerFactory.getLogger(CsvTransformer.class);
	private final ObjectMapper mapper = new ObjectMapper();
	/**
	 * Constructor.
	 *
	 * @param config
	 *            The configuration for the source connector.
	 */
	protected CsvTransformer(SourceCommonConfig config) {
		super(config);
	}

	/**
	 * Creates the stream spliterator for this transformer.
	 *
	 * @param inputStreamIOSupplier
	 *            the input stream supplier.
	 * @param streamLength
	 *            the length of the input stream,
	 *            {@link NativeInfo#UNKNOWN_STREAM_LENGTH} may be used to specify a
	 *            stream with an unknown length, streams of length zero will log an
	 *            error and return an empty stream
	 * @param context
	 *            the context
	 * @return a StreamSpliterator instance.
	 */
	@Override
	protected StreamSpliterator createSpliterator(IOSupplier<InputStream> inputStreamIOSupplier, long streamLength,
			Context<?> context) {
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

					try {
						mapper.readValue(line.getBytes(StandardCharsets.UTF_8), CSVRecord.class);
					} catch(IOException io) {
						LOGGER.warn("Exception trying to parse CSVRecord from bytes, skipping invalid data ",io);
						return true;
					}
					action.accept(toConnectData(line.getBytes(StandardCharsets.UTF_8), null));
					return true;
				} catch (IOException e) {
					LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
					return false;
				}
			}
		};
	}

	/**
	 *
	 * @param value
	 *            A single line from a csv wrapped in a csv commons object
	 *
	 * @param valueSchema
	 *            A schema that matches the value supplied
	 * @return A SchemaAndValue object that can be used by Kafka Connect to send
	 *         Data to Kafka
	 */
	private SchemaAndValue toConnectData(byte[] value, Schema valueSchema) {
		if (value == null) {
			return SchemaAndValue.NULL;
		}

			if (valueSchema == null) {
			valueSchema = SchemaBuilder.OPTIONAL_BYTES_SCHEMA;
		}
		return new SchemaAndValue(valueSchema, value);
	}

}
