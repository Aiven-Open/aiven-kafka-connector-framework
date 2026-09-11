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

package io.aiven.commons.kafka.connector.sink.output;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.aiven.commons.kafka.connector.common.config.FormatType;
import io.aiven.commons.util.io.compression.CompressionType;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class OutputWriter implements AutoCloseable {

    private final SinkOutputStreamWriter writer;

    protected final OutputStream outputStream;

    private Boolean isOutputEmpty;

    private Boolean isClosed;

    protected final Map<String, String> externalConfiguration;

    protected OutputWriter(final OutputStream outputStream, final SinkOutputStreamWriter writer) {
        this(outputStream, writer, Collections.emptyMap());
    }

    protected OutputWriter(final OutputStream outputStream, final SinkOutputStreamWriter writer,
            final Map<String, String> externalConfiguration) {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(outputStream, "outputStream");
        this.writer = writer;
        this.outputStream = outputStream;
        this.externalConfiguration = externalConfiguration;
        this.isOutputEmpty = true;
        this.isClosed = false;
    }

    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        Objects.requireNonNull(sinkRecords, "sinkRecords");
        if (sinkRecords.isEmpty()) {
            return;
        }
        for (final var record : sinkRecords) {
            writeRecord(record);
        }
    }

    public void writeRecord(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        if (this.isOutputEmpty) {
            writer.startWriting(outputStream);
            this.isOutputEmpty = false;
        } else {
            writer.writeRecordsSeparator(outputStream);
        }
        writer.writeOneRecord(outputStream, record);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            try {
                writer.stopWriting(outputStream);
                this.outputStream.flush();
            } finally {
                if (this.outputStream != null) {
                    this.outputStream.close();
                    this.isClosed = true;
                }
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected CompressionType compressionType;

        protected Map<String, String> externalProperties;

        protected Collection<OutputField> outputFields;

        protected boolean envelopeEnabled = true;

        public Builder withCompressionType(final CompressionType compressionType) {
            this.compressionType = compressionType == null ? CompressionType.NONE : compressionType;
            return this;
        }

        public Builder withExternalProperties(final Map<String, String> externalProperties) {
            this.externalProperties = new HashMap<>(externalProperties);
            return this;
        }

        public Builder withOutputFields(final Collection<OutputField> outputFields) {
            this.outputFields = new ArrayList<>(outputFields);
            return this;
        }

        public Builder withEnvelopeEnabled(final Boolean enabled) {
            this.envelopeEnabled = enabled;
            return this;
        }

        public OutputWriter build(final OutputStream outputStream, final FormatType formatType) throws IOException {
            Objects.requireNonNull(outputFields, "Output fields haven't been set");
            Objects.requireNonNull(outputStream, "Output stream hasn't been set");
            return WriterFactory.create(formatType, compressionType, outputFields, outputStream, externalProperties, envelopeEnabled);
        }
    }

}
