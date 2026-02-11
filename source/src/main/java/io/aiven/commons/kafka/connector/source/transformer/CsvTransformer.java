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

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

/**
 * This class provides a transformer to take a List of CSVRecord generated from
 * apache csv-commons and transforms that data into a SchemaAndValue Object
 * usable by Connect to add messages to Kafka.
 */
public class CsvTransformer extends InputStreamTransformer {
	private static final Logger LOGGER = LoggerFactory.getLogger(CsvTransformer.class);
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
				String data = null, header = null;
				try {
					// This is problematic and I need to review.
					while (StringUtils.isBlank(data) || StringUtils.isBlank(header)) {
						header = reader.readLine();
						data = reader.readLine();
						if (header == null && data == null) {
							// end of file
							return false;
						}
					}

					action.accept(toConnectData(header + System.lineSeparator() + data));

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
	 *            A single line from a csv wrapped in String format with the headers
	 *
	 * @return A SchemaAndValue object that can be used by Kafka Connect to send
	 *         Data to Kafka
	 */
	private SchemaAndValue toConnectData(String value) throws IOException {

		if (value == null) {
			return SchemaAndValue.NULL;
		}
		List<CSVRecord> records = CSVFormat.RFC4180.builder().setHeader().get().parse(new StringReader(value))
				.getRecords();
		if (records.size() > 1) {
			throw new RuntimeException(
					String.format("Too many records returned to be transformed: records %s", records.size()));
		}
		CSVRecord record = records.get(0);

		SchemaBuilder valueSchema = SchemaBuilder.struct();

		for (String header : record.getParser().getHeaderNames()) {
			valueSchema.field(header, SchemaBuilder.STRING_SCHEMA);
		}

		// TODO may need update here to auto add in fields, need to check how toMap
		// reacts when headers not provided.

		return new SchemaAndValue(valueSchema, record.toMap());
	}

}
