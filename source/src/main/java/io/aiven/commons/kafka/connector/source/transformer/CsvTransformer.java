/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

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
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class provides a transformer to take a List of CSVRecord generated from
 * apache csv-commons and transforms that data into a SchemaAndValue Object
 * usable by Connect to add messages to Kafka.
 * <p>
 * Assumptions:
 * </p>
 * <ul>
 * <li>CSV is in RFC-4180 format.</li>
 * <li>All columns have unique names, they have no names at all, or overriding
 * names are provided.</li>
 * </ul>
 * <p>
 * If columns have no names they are given the names "field0" to "fieldN".
 * </p>
 * 
 * @see <a href=
 *      "https://www.ietf.org/archive/id/draft-shafranovich-rfc4180-bis-03.html">RFC-4180</a>
 */
public class CsvTransformer extends Transformer {
	/** The schema builder */
	private SchemaBuilder valueSchema;
	/** The configured CSV parser */
	private CSVParser parser;
	/** the configured format for the parser */
	private final CSVFormat csvFormat;
	/** the list of headers for this Transformer */
	private final List<String> headers;

	/**
	 * Gets the registry information for this transformer.
	 * 
	 * @return the registry information for this transformer.
	 */
	public static TransformerRegistry.TransformerInfo info() {
		return new TransformerRegistry.TransformerInfo("CSV", CsvTransformer.class, true,
				"Parses the input bytes as a collection of comma-separated-value records.  Returns one Kafka record for each CSV record as defined in RFC4180.  Records are separated by end-of-line characters.");
	}

	/**
	 * Constructor.
	 *
	 * @param config
	 *            The configuration for the source connector.
	 */
	public CsvTransformer(SourceCommonConfig config) {
		super(config);
		CSVFormat.Builder builder = CSVFormat.RFC4180.builder();
		if (config.isCsvTransformerHeaderEnabled()) {
			builder.setHeader().setSkipHeaderRecord(true);
		}
		headers = config.getCsvTransformerHeader();
		csvFormat = builder.get();
	}

	/**
	 * Convert the native key into a Schema and Value for Kafka.
	 *
	 * @param evolvingSourceRecord
	 *            the evolving source record to build the keyData from.
	 * @return a SchemaAndValue for the key.
	 */
	@Override
	public SchemaAndValue generateKeyData(final EvolvingSourceRecord evolvingSourceRecord) {
		return SchemaAndValueFactory.createSchemaAndValue(evolvingSourceRecord.getNativeKey());
	}

	/**
	 * Gets a stream of SchemaAndValue records from the input stream.
	 *
	 * @param sourceRecord
	 *            The evolving source regord being processed.
	 * @return the stream of values for Kafka SourceRecords.
	 */
	@Override
	public Stream<SchemaAndValue> generateRecords(final EvolvingSourceRecord sourceRecord) {

		try (InputStream inputStream = sourceRecord.getInputStream().get()) {
			return getRecords(IOUtils.toString(inputStream, StandardCharsets.UTF_8)).stream()
					.skip(sourceRecord.getRecordCount()).map(this::toConnectData);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read input stram for " + sourceRecord.getNativeKey(), e);
		}

	}

	private List<CSVRecord> getRecords(String sourceRecord) {
		try {
			parser = csvFormat.parse(new StringReader(sourceRecord));
			return parser.getRecords();
		} catch (IOException e) {
			throw new RuntimeException(String.format("IOException occurred when processing csv to CSVRecord %s", e));
		}
	}

	private List<String> constructHeaders(List<String> parserHeaders) {
		if (headers == null) {
			return new ArrayList<>(parserHeaders);
		}
		List<String> result = new ArrayList<>(headers);
		if (parserHeaders != null && headers.size() < parserHeaders.size()) {
			result.addAll(parserHeaders.subList(headers.size(), parserHeaders.size()));
		}
		return result;
	}

	private void createValueSchema(CSVRecord record) {
		valueSchema = SchemaBuilder.struct();
		List<String> headers = constructHeaders(parser.getHeaderNames());

		int limit = Math.max(headers.size(), record.size());
		for (int i = 0; i < limit; i++) {
			if (i < headers.size()) {
				valueSchema.field(headers.get(i), SchemaBuilder.STRING_SCHEMA);
			} else {
				String name = "field" + i;
				valueSchema.field(name, SchemaBuilder.STRING_SCHEMA);
			}
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
		final Map<String, String> output = new LinkedHashMap<>(value.size());

		if (valueSchema == null || valueSchema.fields().size() < value.size()) {
			createValueSchema(value);
		}

		List<Field> fields = valueSchema.fields();
		for (int i = 0; i < fields.size(); i++) {
			// handle the case where there are fewer fields than headers.
			if (i < value.size()) {
				output.put(valueSchema.fields().get(i).name(), value.get(i));
			} else {
				output.put(valueSchema.fields().get(i).name(), "");
			}
		}

		return new SchemaAndValue(valueSchema, output);
	}

	@Override
	public void close() throws Exception {
		if (parser != null) {
			parser.close();
			parser = null;
		}
		valueSchema = null;
	}
}
