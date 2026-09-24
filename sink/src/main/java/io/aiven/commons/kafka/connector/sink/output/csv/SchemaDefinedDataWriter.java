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
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A byte array writer that may perform encoding of the byte array before writing.
 */
public class SchemaDefinedDataWriter implements Function<SinkRecord, Object>  {
    private static final List<Schema.Type> UNSUPPORTED_TYPES = List.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.STRUCT);

    /**
     * The encoder for the byte array.
     */
    private final Function<byte[], byte[]> encoder;
    private final Function<SinkRecord, Object> source;
    private final Function<SinkRecord, Schema> schema;



    /**
     * Creates an array writer that will write with the specified encoding.
     * @param encoding the encoding to write with.
     */
    public SchemaDefinedDataWriter(final OutputFieldEncodingType encoding, Function<SinkRecord, Object> source, final Function<SinkRecord,Schema> schema) {
        this(encoding.encoder, source, schema);
    }

    /**
     * Creates an array writer that will write with the specified encoding.
     * @param encoder the encoding function for the byte array.
     */
    public SchemaDefinedDataWriter(final Function<byte[], byte[]> encoder, final Function<SinkRecord, Object> source, final Function<SinkRecord, Schema> schema) {
        this.encoder = encoder;
        this.source = source;
        this.schema = schema;
    }

    /**
     * Takes the {@link SinkRecord}'s value as a byte array.
     *
     * <p>
     * If the value is {@code null}, it outputs nothing.
     *
     * <p>
     * If the value is not {@code null}, it assumes the value <b>is</b> a byte array.
     *
     * @param record
     *            the record to get the value from
     * @throws DataException
     *             when the value is not actually a byte array
     */
    @Override
    public String apply(final SinkRecord record) throws DataException {
        Objects.requireNonNull(record, "record cannot be null");

        Schema schemaValue = schema.apply(record);
//        if (schemaValue != null && schemaValue.type() != Schema.Type.BYTES) {
//            throw new DataException(String.format("Schema type must be %s, %s given", Schema.Type.BYTES,
//                    schemaValue.type()));
//        }
//
        Object value = source.apply(record);
//        // Do nothing if the value is null.
//       return data == null ? null : new String(encoder.apply(data));

        if (schemaValue != null && UNSUPPORTED_TYPES.contains(schemaValue.type())) {
                        throw new DataException(String.format("Schema type must not be in %s. '%s' given. ", UNSUPPORTED_TYPES, schemaValue.type()));
        }

        if (value != null) {
            if (schemaValue == null) {
                return new String(encoder.apply(((byte[]) value)));
            } else {
                return new String(encoder.apply(value.toString().getBytes(StandardCharsets.UTF_8)));
            }
        }
        return null;
    }

}
