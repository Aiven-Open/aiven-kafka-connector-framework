package io.aiven.commons.kafka.connector.sink.output;

import io.aiven.commons.kafka.connector.common.config.FormatType;
import io.aiven.commons.kafka.connector.sink.output.avro.AvroOutputWriter;
import io.aiven.commons.kafka.connector.sink.output.csv.CsvOutputWriter;
import io.aiven.commons.kafka.connector.sink.output.jsonwriter.JsonLinesOutputWriter;
import io.aiven.commons.kafka.connector.sink.output.jsonwriter.JsonOutputWriter;
import io.aiven.commons.kafka.connector.sink.output.parquet.ParquetOutputWriter;
import io.aiven.commons.util.io.compression.CompressionType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

class WriterFactory {
    public static OutputWriter create(final FormatType format, final CompressionType compressionType, final Collection<OutputField> fields, final OutputStream outputStream,
                        final Map<String, String> externalConfig, final boolean envelopeEnabled) throws IOException {
        return switch (format) {
            case AVRO -> new AvroOutputWriter(fields, compressionType.compress(outputStream), externalConfig, envelopeEnabled);


            case CSV -> new CsvOutputWriter(fields, compressionType.compress(outputStream));

            case JSON -> new JsonOutputWriter(fields, compressionType.compress(outputStream), envelopeEnabled);

            case JSONL -> new JsonLinesOutputWriter(fields, compressionType.compress(outputStream), envelopeEnabled);

            case PARQUET -> new ParquetOutputWriter(fields, outputStream, compressionType, externalConfig, envelopeEnabled);
        };
    }
}
