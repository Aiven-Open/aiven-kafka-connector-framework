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

package io.aiven.commons.kafka.connector.sink.output.csv;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import io.aiven.commons.kafka.connector.sink.output.OutputField;
import io.aiven.commons.kafka.connector.sink.output.OutputWriter;
import org.apache.kafka.connect.sink.SinkRecord;


public final class CsvOutputWriter extends OutputWriter {

    public CsvOutputWriter(final Collection<OutputField> fields, final OutputStream outputStream) {
        super(outputStream, new Builder().addFields(fields).build());
    }

    static final class Builder {
        private final List<Function<SinkRecord, Object>> writers = new ArrayList<>();

        Builder addFields(final Collection<OutputField> fields) {
            Objects.requireNonNull(fields, "fields cannot be null");
            for (final OutputField field : fields) {
                switch (field.getFieldType()) {
                    case KEY -> {
                        writers.add(new SchemaDefinedDataWriter(Function.identity(), SinkRecord::key, SinkRecord::keySchema));
                    }

                    case VALUE ->
                        writers.add(new SchemaDefinedDataWriter(field.getEncodingType(), SinkRecord::value, SinkRecord::valueSchema));

                    case OFFSET ->
                        writers.add(SinkRecord::kafkaOffset);

                    case ORIGINAL_OFFSET ->
                        writers.add(SinkRecord::originalKafkaOffset);

                    case TIMESTAMP ->
                        writers.add(SinkRecord::timestamp);

                    case HEADERS ->
                        writers.add(new CsvHeaderWriter());

                }
            }

            return this;
        }

        CsvOutputStreamWriter build() {
            return new CsvOutputStreamWriter(writers);
        }
    }
}
