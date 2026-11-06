/*
 * Copyright 2020 Aiven Oy
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

import io.aiven.commons.kafka.connector.sink.output.OutputFieldEncodingType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class CsvHeaderWriter implements Function<SinkRecord, Object> {
    private static final String HEADER_KEY_VALUE_SEPARATOR = ":";
    private static final String HEADERS_SEPARATOR = ";";

    private static final List<Schema.Type> UNSUPPORTED_TYPES = List.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.STRUCT);
    @Override
    public String apply(final SinkRecord record) throws DataException {
        Objects.requireNonNull(record, "record cannot be null");

        StringWriter stringWriter = new StringWriter();
        for (final Header header : record.headers()) {
            final String topic = record.topic();
            final String key = header.key();
            final Schema schema = header.schema();
            final Object value = header.value();
            if (schema != null && UNSUPPORTED_TYPES.contains(schema)) {
                throw new DataException(String.format("Invalid schema type for CSV Header '%s' value: %s", key, schema.type()));
            }
            stringWriter.append(new String(OutputFieldEncodingType.BASE64.encoder.apply(key.getBytes(StandardCharsets.UTF_8))));
            stringWriter.append(HEADER_KEY_VALUE_SEPARATOR);
            if (value != null) {
                if (schema == null) {
                    stringWriter.append(new String(OutputFieldEncodingType.BASE64.encoder.apply(((byte[]) value))));
                } else {
                    stringWriter.append(new String(OutputFieldEncodingType.BASE64.encoder.apply(value.toString().getBytes(StandardCharsets.UTF_8))));
                }
            }
            stringWriter.append(HEADERS_SEPARATOR);

        }
        return stringWriter.toString();
    }
}
