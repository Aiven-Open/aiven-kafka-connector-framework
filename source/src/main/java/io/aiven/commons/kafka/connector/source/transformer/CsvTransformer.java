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

import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class provides a transformer to take a List of CSVRecord generated from
 * apache csv-commons and transforms that data into a SchemaAndValue Object
 * usable by Connect to add messages to Kafka.
 */
public class CsvTransformer extends Transformer {
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
	 * Convert the native key into a Schema and Value for Kafka.
	 *
	 * @param evolvingSourceRecord
	 *            the abstract source record to extract the keyData from.
	 * @return a SchemaAndValue for the key.
	 */
	@Override
	public SchemaAndValue generateKeyData(EvolvingSourceRecord evolvingSourceRecord) {
		return evolvingSourceRecord.getKey();
	}

	/**
	 * Gets a stream of SchemaAndValue records from the input stream.
	 *
	 * @param sourceRecord
	 *            The AbstractSourceRecord being processed.
	 * @return the stream of values for Kafka SourceRecords.
	 */
	@Override
	public Stream<SchemaAndValue> generateRecords(final EvolvingSourceRecord sourceRecord) {

		try {
			return getRecords(IOUtils.toString(sourceRecord.getInputStream(), StandardCharsets.UTF_8)).stream()
					.map(this::toConnectData);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private static List<CSVRecord> getRecords(String sourceRecord) {
		try {
			return CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).get()
					.parse(new StringReader(sourceRecord)).getRecords();
		} catch (IOException e) {
			throw new RuntimeException(String.format("IOException occurred when processing csv to CSVRecord %s", e));
		}
	}

	/**
	 *
	 * @param value
	 *            A single line from a csv wrapped in String format with the headers
	 *
	 * @return A SchemaAndValue object that can be used by Kafka Connect to send
	 *         Data to Kafka
	 */
	private SchemaAndValue toConnectData(CSVRecord value) {

		SchemaBuilder valueSchema = SchemaBuilder.struct();

		for (String header : value.getParser().getHeaderNames()) {
			valueSchema.field(header, SchemaBuilder.STRING_SCHEMA);
		}

		// TODO may need update here to auto add in fields, need to check how toMap
		// reacts when headers not provided.

		return new SchemaAndValue(valueSchema, value.toMap());
	}

}