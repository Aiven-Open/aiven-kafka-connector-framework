/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.commons.kafka.connector.sink.output.parquet;

import io.aiven.commons.kafka.connector.sink.output.OutputField;
import io.aiven.commons.kafka.connector.sink.output.OutputWriter;
import io.aiven.commons.kafka.connector.sink.output.SinkOutputStreamWriter;
import io.aiven.commons.kafka.connector.sink.output.SinkRecordConverter;
import io.aiven.commons.kafka.connector.sink.output.avro.AvroSchemaBuilder;

import io.aiven.commons.util.io.compression.CompressionType;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class ParquetOutputWriter extends OutputWriter {

    private static final String NAMESPACE = "io.aiven.parquet.output.schema";

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetOutputWriter.class);

    private final SinkRecordConverter sinkRecordConverter;

    private final AvroSchemaBuilder schemaBuilder;

    private final CompressionType compressionType;

    public ParquetOutputWriter(final Collection<OutputField> fields, final OutputStream out, final CompressionType compressionType,
                               final Map<String, String> externalConfig, final boolean envelopeEnabled) {
        super(new ParquetPositionOutputStream(out), new OutputStreamWriterStub(), externalConfig);
        this.compressionType = compressionType;
        final var avroData = new AvroData(new AvroDataConfig(externalConfig));
        this.sinkRecordConverter = new SinkRecordConverter(fields, avroData, envelopeEnabled);
        this.schemaBuilder = new AvroSchemaBuilder(NAMESPACE, fields, avroData, envelopeEnabled);
    }

    @Override
    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        final var parquetConfig = new ParquetConfig(externalConfiguration);
        final var parquetSchema = schemaBuilder.buildSchema(sinkRecords.iterator().next());
        LOGGER.debug("Record schema is: {}", parquetSchema);
        try (ParquetWriter parquetWriter = AvroParquetWriter.builder(new ParquetOutputFile())
                .withSchema(parquetSchema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withDictionaryEncoding(true)
                .withConf(parquetConfig.parquetConfiguration())
                .withCompressionCodec(ParquetConfig.compressionCodecName(compressionType))
                .build()) {
            for (final var record : sinkRecords) {
                parquetWriter.write(sinkRecordConverter.convert(record, parquetSchema));
            }
        }
    }

    @Override
    public void writeRecord(final SinkRecord record) throws IOException {
        this.writeRecords(List.of(record));
    }

    private static final class OutputStreamWriterStub implements SinkOutputStreamWriter {
        @Override
        public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        }
    }

    private class ParquetOutputFile implements OutputFile {

        @Override
        public PositionOutputStream create(final long blockSizeHint) throws IOException {
            return (ParquetPositionOutputStream) outputStream;
        }

        @Override
        public PositionOutputStream createOrOverwrite(final long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

}
